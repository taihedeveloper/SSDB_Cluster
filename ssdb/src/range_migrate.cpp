#include <map>
#include "range_migrate.h"
#include "net/link.h"
#include "net/fde.h"
#include "net/resp.h"
#include "ssdb/binlog2.h"
#include "util/log.h"
#include "util/spin_lock.h"
#include "ssdb/version.h"

#define RANGE_MIGRATE_SEQ 0
#define RANGE_MIGRATE_RECV_TIMEOUT 10000
#define RANGE_MIGRATE_SYNCIO_RESOLUTION 200

static const std::vector<Bytes> *sync_read(Fdevents *evb, Link *link, int64_t timeout_ms);

RangeMigrate::RangeMigrate(SSDB_BinLog *binlog, SSDB *ssdb, ExpirationHandler *expiration, SegKeyLock &lock)
	:binlog(binlog), ssdb(ssdb), expiration(expiration), key_lock(lock), migrate_ret(0){
}

RangeMigrate::~RangeMigrate() {
}

void RangeMigrate::migrate(Link *link, Response *resp, const std::string &ip, int port,
		const std::string &start, const std::string &end, int speed, int64_t timeout_ms) {
	if(processing == 1) {
		/* there is a migrating thread running */
		link->send("tryagain");
		if(link->flush() == -1) {
			log_warn("range migrate reply failed");
		}
		SAFE_DELETE(link);
		return;
	}

	processing = 1;
	pthread_t tid = 0;
	struct _thread_args *arg = new struct _thread_args();
	arg->owner = this;
	arg->upstream = link;
	arg->ip = ip;
	arg->port = port;
	arg->start = start;
	arg->end = end;
	arg->speed = speed;
	arg->timeout = timeout_ms < 0 ? 0 : timeout_ms;
	arg->resp = resp;

	int err = pthread_create(&tid, NULL, &RangeMigrate::_migrate_thread, static_cast<void*>(arg));
	if (err != 0) {
		log_error("can't start migrate thread: %s", strerror(err));
		processing = 0;
		delete arg;
	}
	pthread_join(tid, NULL);
}

void RangeMigrate::import(Link *link, const std::string &sync_key) {
	pthread_t tid = 0;
	struct _thread_args *arg = new struct _thread_args();
	arg->owner = this;
	arg->link = link;
	arg->sync_key = sync_key;
	int err = pthread_create(&tid, NULL, &RangeMigrate::_import_thread, static_cast<void*>(arg));
	if(err != 0) {
		log_error("can't start improt thread: %s", strerror(err));
		delete arg;
	}
}

int RangeMigrate::get_migrate_ret() {
	return migrate_ret;
}

std::string RangeMigrate::get_migrate_msg(int n) {
	switch(n) {
		case 0:
			return "DONE";
		case 1:
			return "PROCESSING";
		case -1:
			return "ERROR";
		default:
			return "UNKNOWN";
	}
}

void *RangeMigrate::_migrate_thread(void *arg) {
	//pthread_detach(pthread_self());
	SET_PROC_NAME("migrator");
	struct _thread_args *p = static_cast<struct _thread_args *>(arg);

	Client c;
	c.owner = p->owner;
	c.upstream = p->upstream;
	c.resp = p->resp;
	c.ip = p->ip;
	c.port = p->port;
	c.start = p->start;
	c.end = p->end;
	c.sync_speed = p->speed;
	c.proc_timeout = p->timeout;
	c.proc_start = time_ms();
	c.send_count = 0;
	delete p;

	log_info("range migrating start");
	c.proc();
	log_info("range migrating quit, %" PRIu64 " keys sent", c.send_count);
	return NULL;
}

void *RangeMigrate::_import_thread(void *arg) {
	pthread_detach(pthread_self());
	SET_PROC_NAME("importor");
	struct _thread_args *p = static_cast<struct _thread_args *>(arg);

	Server s;
	s.owner = p->owner;
	s.link = p->link;
	s.sync_key = p->sync_key;
	delete p;

	log_info("range importing start");
	s.proc();
	log_info("range importing quit");
	return NULL;
}

RangeMigrate::Client::Client() : link(NULL), status(CLIENT_DISCONNECT), ttl(-1){
}

RangeMigrate::Client::~Client() {
	SAFE_DELETE(link);
}

void RangeMigrate::Client::connect() {
	link = Link::connect(ip.c_str(), port);
	if(link == NULL) {
		log_error("failed to connect to migration destination: %s:%d", ip.c_str(), port, strerror(errno));
		status = CLIENT_RECONNECT;
		return;
	}
	log_info("connect to %s:%d", ip.c_str(), port);
	status = CLIENT_CONNECTED;
}

void RangeMigrate::Client::reconnect() {
	if(link) {
		select.del(link->fd());
	}
	SAFE_DELETE(link);
	sleep(1);
	status = CLIENT_DISCONNECT;
}

#define KEY_LOCK_DELETE_KEY(lock, key) \
do { \
	WriteLockGuard<KeyLock> guard((lock).get_key_lock(key)); \
	lock.del_key(key); \
} while(0)

int RangeMigrate::Client::key_migrate_init() {
	/**
	 * disable write while generate migrate key, which is
	 * the first key in the range between 'start' and 'end'.
	 * lock is needed, as it must be sure the key invariant
	 * before we add the sync_key to key-lock-set
	 **/
	WriteLockGuard<SegKeyLock> guard(owner->key_lock);
	log_debug("key migrate init, start: %s end: %s",
		hexmem(start.c_str(), start.size()).c_str(), hexmem(end.c_str(), end.size()).c_str());
	Iterator *iter = owner->ssdb->iterator(start, end, UINT64_MAX);
	bool flag = false;
	while(iter->next()) {
		if(decode_version_key(iter->key(), &sync_key) == -1) {
			log_error("decode version key failed: %s", hexmem(iter->key().data(), iter->key().size()).c_str());
			SAFE_DELETE(iter);
			return -1;
		}
		log_debug("sync_key: %s", hexmem(sync_key.data(), sync_key.size()).c_str());
		if(owner->ssdb->get_version(sync_key, &sync_type, &sync_version) == -1) {
			SAFE_DELETE(iter);
			return -1;
		}

		if(sync_type != DataType::KV && sync_type != DataType::SET
				&& sync_type != DataType::ZSET && sync_type != DataType::HASH && sync_type != DataType::QUEUE) {
			log_error("unknown data type: %c", sync_type);
			SAFE_DELETE(iter);
			return -1;
		}

		/* reset all args */
		sync_field = "";
		sync_qcount = 0;
		flag = true;
		break;
	}

	SAFE_DELETE(iter);

	if(!flag) {
		/* no more key to migrate */
		return 0;
	}
	log_debug("first key: %s", hexmem(sync_key.data(), sync_key.size()).c_str());
	owner->key_lock.add_key(sync_key);
	return 1;
}

#define SSDB_RANGE_MIGRATE_CHECK_KEY(k1, k2) \
do {\
	if(k1 != k2) {\
		log_error("psync unexpected key: %s, expected: %s",\
				hexmem((k1).data(), (k1).size()).c_str(), hexmem((k2).data(), (k2).size()).c_str());\
		return -1;\
	}\
} while(0)

int RangeMigrate::Client::adjust(const LogEvent &log) {
	std::string raw = log.key().String();
	uint64_t s, v;
	std::string k;
	std::string f;
	if(raw[0] != sync_type) {
		log_error("psync unexpected data type: %c, expected: %c", raw[0], sync_type);
		return -1;
	}
	switch(raw[0]) {
		case DataType::KV:
			if(decode_kv_key(raw, &k, &v) == -1) {
				return -1;
			}
			SSDB_RANGE_MIGRATE_CHECK_KEY(k, sync_key);
			break;
		case DataType::SET:
			if(decode_set_key(raw, &k, &f, &v) == -1) {
				return -1;
			}
			SSDB_RANGE_MIGRATE_CHECK_KEY(k, sync_key);
			sync_field = f;
			break;
		case DataType::HASH:
			if(decode_hash_key(raw, &k, &f, &v) == -1) {
				return -1;
			}
			SSDB_RANGE_MIGRATE_CHECK_KEY(k, sync_key);
			sync_field = f;
			break;
		case DataType::ZSET:
			if(decode_zset_key(raw, &k, &f, &v) == -1) {
				return -1;
			}
			SSDB_RANGE_MIGRATE_CHECK_KEY(k, sync_key);
			sync_field = f;
			break;
		case DataType::QUEUE:
			if(decode_qitem_key(raw, &k, &s, &v) == -1) {
				return -1;
			}
			SSDB_RANGE_MIGRATE_CHECK_KEY(k, sync_key);
			sync_qcount = int64_t(s);
			break;
		default:
			log_error("unknown data type: %c", raw[0]);
			return -1;
	}
	ttl = owner->expiration->get_ttl(sync_key);
	return 0;
}

void RangeMigrate::Client::init() {
	log_debug("client init");
	LogEvent log;
	const std::vector<Bytes> *req = NULL;
	status = CLIENT_INITIALIZED;
	/* get the first key to send */
	if(key_migrate_init() == 0) {
		log_info("there is no key to sync");
		status = CLIENT_DONE;
		return;
	}

	/* key is locked. Now, send the key */
	link->send("migrate", sync_key);
	link->noblock();
	if(link->flush() == -1) {
		log_error("send cmd migrate error: %s", strerror(errno));
		status = CLIENT_RECONNECT;
		goto unlock_key;
	}
	req = sync_read(&select, link, RANGE_MIGRATE_RECV_TIMEOUT);
	if(req == NULL || req->empty()) {
		log_error("link recv error: %s errno: %d , reconnect to migration target", strerror(errno), errno);
		status = CLIENT_RECONNECT;
		goto unlock_key;
	}

	if((*req)[0].size() == 1) {
		/* (*req)[0].data()[0] == '-' */
		log_error("destination rejects importing, migration abort");
		status = CLIENT_ABORT;
		goto unlock_key;
	}

	if(log.load((*req)[0]) == -1) {
		log_error("range migration response binlog error, migration abort");
		status = CLIENT_ABORT;
		goto unlock_key;
	}
	if(log.cmd() != BinlogCommand::BEGIN) {
		log_error("range migration unexpected command: %" PRIu8 ", migration abort", (unsigned char)log.cmd());
		status = CLIENT_ABORT;
		goto unlock_key;
	}

	return;
	/* disable inner key psync for possible inconsistency */
	/* check if need to adjust iterator */
	/*if(!log.key().empty()) {
		log_debug("recvieve key: %s", hexmem(log.key().data(), log.key().size()).c_str());
		if(adjust(log) != 0) {
			log_warn("range migration can't adjust to the key: %s, migration target need to delete key",
				hexmem(log.key().data(), log.key().size()).c_str());
		}
		log_debug("psync initilized");
		status = CLIENT_COPY;
		return;
	}*/

unlock_key:
	log_debug("delete key in key lock for status %d", status);
	KEY_LOCK_DELETE_KEY(owner->key_lock, sync_key);
}

int RangeMigrate::Client::flush() {
	float data_size_mb = link->output->size() / 1024.0 / 1024.0;
	link->noblock();
	if(link->flush() == -1) {
		log_info("%s: %d fd:%d, send error: %s",
			link->remote_ip, link->remote_port, link->fd(), strerror(errno));
		status = CLIENT_RECONNECT;
		return -1;
	}
	if(sync_speed > 0) {
		usleep(static_cast<uint64_t>(data_size_mb / sync_speed) * 1000 * 1000);
	}
}

int RangeMigrate::Client::copy_kv() {
	if(status != CLIENT_COPY) {
		/* key not sent yet */
		std::string value;
		int ret = owner->ssdb->get(sync_key, &value, sync_version);
		if(ret == -1) {
			return -1;
		}
		LogEvent log(RANGE_MIGRATE_SEQ, BinlogType::COPY, BinlogCommand::KSET, sync_key);
		link->send(log.repr(), value);
	}
	return 0;
}

int RangeMigrate::Client::copy_set() {
	SIterator *it = owner->ssdb->sscan(sync_key, sync_field, UINT64_MAX, sync_version);
	if(status == CLIENT_COPY) {
		/* skip the first key, as it is psync */
		it->next();
	}
	int iterator_count = 0;
	while(it->next()) {
		if(++iterator_count > 1000 || link->output->size() > 2 * 1024 * 1024) {
			if(flush() == -1) {
				return -1;
			}
		}
		LogEvent log(RANGE_MIGRATE_SEQ, BinlogType::COPY, BinlogCommand::SSET, sync_key);
		link->send(log.repr(), it->elem);
	}
	return 0;
}

int RangeMigrate::Client::copy_zset() {
	ZIterator *it = owner->ssdb->zscan(sync_key, sync_field, "", "", UINT64_MAX, sync_version);
	if(status == CLIENT_COPY) {
		/* skip the first key, as it is psync */
		it->next();
	}
	int iterator_count = 0;
	while(it->next()) {
		if(++iterator_count > 1000 || link->output->size() > 2 * 1024 *1024) {
			if(flush() == -1) {
				return -1;
			}
		}
		LogEvent log(RANGE_MIGRATE_SEQ, BinlogType::COPY, BinlogCommand::ZSET, encode_zset_key_ex(it->key, it->field));
		link->send(log.repr(), it->score);
	}
	return 0;
}

int RangeMigrate::Client::copy_hash() {
	HIterator *it = owner->ssdb->hscan(sync_key, sync_field, "", UINT64_MAX, sync_version);
	if(status == CLIENT_COPY) {
		/* skip the first key, as it is psync */
		it->next();
	}
	int iterator_count = 0;
	while(it->next()) {
		if(++iterator_count > 1000 || link->output->size() > 2 * 1024 * 1024) {
			if(flush() == -1) {
				return -1;
			}
		}
		LogEvent log(RANGE_MIGRATE_SEQ, BinlogType::COPY, BinlogCommand::HSET, encode_hash_key_ex(it->key, it->field));
		link->send(log.repr(), it->val);
	}
	return 0;
}

int RangeMigrate::Client::copy_queue() {
	/* get queue front seq */
	std::string qfront_raw_key = encode_qitem_key(sync_key, QFRONT_SEQ, sync_version);
	std::string front_seq_str;
	if(owner->ssdb->raw_get(qfront_raw_key, &front_seq_str) <= 0) {
		log_error("get front seq failed");
		status = CLIENT_ABORT;
		return -1;
	}
	uint64_t front_seq = *reinterpret_cast<const uint64_t*>(front_seq_str.data());
	/* create iterator */
	QIterator *it = owner->ssdb->qscan(sync_key, front_seq-1, UINT64_MAX, sync_version);
	it->skip(sync_qcount);
	int iterator_count = 0;
	while(it->next()) {
		log_debug("sync_qcount: %" PRIu64 " %s", sync_qcount, (it->val).c_str());
		if(++iterator_count > 1000 || link->output->size() > 2 * 1024 * 1024) {
			if(flush() == -1) {
				return -1;
			}
		}
		LogEvent log(RANGE_MIGRATE_SEQ, BinlogType::COPY, BinlogCommand::QPUSH_BACK, sync_key);
		link->send(log.repr(), it->val);
	}
	return 0;
}

void RangeMigrate::Client::copy() {
	int ret = 0;
	switch(sync_type) {
		case DataType::KV:
			ret = copy_kv();
			break;
		case DataType::SET:
			ret = copy_set();
			break;
		case DataType::ZSET:
			ret = copy_zset();
			break;
		case DataType::HASH:
			ret = copy_hash();
			break;
		case DataType::QUEUE:
			ret = copy_queue();
			break;
		default:
			ret = -1;
	}
	if(ret == 0) {
		/* send ttl and require ack */
		ttl = owner->expiration->get_ttl(sync_key);
		LogEvent log(RANGE_MIGRATE_SEQ, BinlogType::COPY, BinlogCommand::ACK, sync_key, ttl);
		link->send(log.repr(), "ack");
		/* flush every time a key sent */
		status = CLIENT_ACK;
		ret = flush();
	}
	if(ret == -1) {
		/* unlock key */
		KEY_LOCK_DELETE_KEY(owner->key_lock, sync_key);
	}
}

void RangeMigrate::Client::confirm(char cmd) {
	const std::vector<Bytes> *req = sync_read(&select, link, RANGE_MIGRATE_RECV_TIMEOUT);
	if(req == NULL) {
		log_error("link recv error: %s, reconnect to migration target", strerror(errno));
		status = CLIENT_RECONNECT;
		return;
	}
	LogEvent log;
	if(log.load((*req)[0]) == -1) {
		log_error("invalid binlog, range adjust failed, migration abort");
		status = CLIENT_ABORT;
		return;
	}
	if(log.cmd() != cmd) {
		log_error("invalid ack, migration abort");
		status = CLIENT_ABORT;
		return;
	}

	if(cmd == BinlogCommand::ACK) {
		Transaction trans(owner->ssdb, sync_key);
		if(owner->ssdb->del(sync_key, trans) == -1) {
			log_error("range migrate client delete key: %s failed, migration abort",
				hexmem(sync_key.c_str(), sync_key.size()).c_str());
			status = CLIENT_ABORT;
			return;
		}
		owner->expiration->del_ttl(sync_key);
		if(owner->binlog) {
			owner->binlog->write(BinlogType::SYNC, BinlogCommand::K_DEL, sync_key);
		}

		KEY_LOCK_DELETE_KEY(owner->key_lock, sync_key);
		status = CLIENT_INITIALIZED;
		if(key_migrate_init() == 0) {
			LogEvent log(RANGE_MIGRATE_SEQ, BinlogType::COPY, BinlogCommand::END);
			link->send(log.repr(), "sync_end");
			int ret = flush();
			if (ret == -1) {
				log_error("range migrate client send sync_end failed");
				status = CLIENT_ABORT;
			} else {
				status = CLIENT_EOF;
			}
		}
		++send_count;
	} else if (cmd == BinlogCommand::END){
		KEY_LOCK_DELETE_KEY(owner->key_lock, sync_key);
		status = CLIENT_DONE;
	} else {
		log_error("unexpected command: %c, migration abort", cmd);
		KEY_LOCK_DELETE_KEY(owner->key_lock, sync_key);
		status = CLIENT_ABORT;
	}
}

void RangeMigrate::Client::confirm_ack() {
	confirm(BinlogCommand::ACK);
}

void RangeMigrate::Client::confirm_eof() {
	confirm(BinlogCommand::END);
}

void RangeMigrate::Client::proc() {
	owner->migrate_ret = 1;
	std::string msg;
	while(true) {
		switch(status) {
			case CLIENT_DISCONNECT:
				connect();
				break;
			case CLIENT_RECONNECT:
				reconnect();
				break;
			case CLIENT_CONNECTED:
				init();
				break;
			case CLIENT_INITIALIZED:
			case CLIENT_COPY:
				copy();
				break;
			case CLIENT_ACK:
				confirm_ack();
				break;
			case CLIENT_EOF:
				confirm_eof();
				break;
			case CLIENT_ABORT:
				owner->migrate_ret = -1;
				msg = "abort";
				goto finish;
			case CLIENT_DONE:
				owner->migrate_ret = 0;
				msg = "done";
				goto finish;
			default:
				log_warn("unknown status");
				owner->migrate_ret = -1;
				msg = "abort";
				goto finish;
		}

		/* proc timeout only after full key migration */
		if(status == CLIENT_INITIALIZED && proc_timeout > 0 && time_ms() - proc_start > proc_timeout) {
			log_info("range migrate timeout");
			owner->migrate_ret = 0;
			msg = "continue";
			goto finish;
		}
	}

finish:
	/* block process, do not flush link here */
	resp->push_back(msg);
	owner->processing = 0;
}


RangeMigrate::Server::Server() {
}

RangeMigrate::Server::~Server() {
	SAFE_DELETE(link);
}

void RangeMigrate::Server::proc() {
	log_info("start range importing form %s:%d", link->remote_ip, link->remote_port);
	if (init() != 0) {
		log_error("range importing alignment failed");
		goto err;
	}
	while(true) {
		const std::vector<Bytes> *req = sync_read(&select, link, RANGE_MIGRATE_RECV_TIMEOUT);
		if(req == NULL) {
			log_error("link recv error: %s, importing abort", strerror(errno));
			goto err;
		} else {
			int ret = proc_req(*req);
			if(ret == 1) {
				log_info("range importe done");
				return;
			} else if(ret != 0) {
				log_error("process request failed");
				goto err;
			}
		}
	}
err:
	log_error("range import failed");
}

int RangeMigrate::Server::init() {
	std::string ack_key = "";
	int ret = owner->ssdb->get_version(Bytes(sync_key), &sync_type, &sync_version);
	if(ret == -1) {
		return -1;
	}
	first = true;
	/* possible data inconsistency in psync if source key is changed after migrating intrrupted,
	 * maybe a batter psync way would come up in continuous improvement.
	 */
	/*if(ret != 0) {
		switch(sync_type) {
			case DataType::KV:
				ack_key = encode_kv_key(sync_key, sync_version);
				break;
			case DataType::SET:
				{
					SIterator *sit = owner->ssdb->srscan(sync_key, "", 1, sync_version);
					if(sit->next()) {
						ack_key = encode_set_key(sync_key, sit->elem, sync_version);
					}
					delete sit;
				}
				break;
			case DataType::ZSET:
				{
					ZIterator *zit = owner->ssdb->zrscan(sync_key, "", "", "", 1, sync_version);
					if(zit->next()) {
						ack_key = encode_zset_key(sync_key, zit->field, sync_version);
					}
					delete zit;
				}
				break;
			case DataType::HASH:
				{
					HIterator *hit = owner->ssdb->hrscan(sync_key, "", "", 1, sync_version);
					if(hit->next()) {
						ack_key = encode_hash_key(sync_key, hit->field, sync_version);
					}
					delete hit;
				}
				break;
			case DataType::QUEUE:
				{
					int64_t size = owner->ssdb->qsize(sync_key, sync_version);
					if(size > 0) {
						ack_key = encode_qitem_key(sync_key, uint64_t(size), sync_version);
					}
				}
				break;
			default:
				log_error("unknown data type: %c", sync_type);
				return -1;
		}
	}*/

	/* send command 'begin' */
	ret = send_cmd(BinlogType::COPY, BinlogCommand::BEGIN, ack_key, "range_sync_begin");
	if(ret != 0) {
		log_error("range importing send command 'sync_begin' error: %s", strerror(errno));
		return -1;
	}
	log_debug("command begin sent");
	return 0;
}

int RangeMigrate::Server::init_meta(Transaction &trans) {
	if(owner->ssdb->del(sync_key, trans) == -1) {
		log_error("del %s failed", hexmem(sync_key.data(), sync_key.size()).c_str());
		return -1;
	}
	owner->expiration->del_ttl(sync_key);
	if(owner->binlog) {
		owner->binlog->write(BinlogType::SYNC, BinlogCommand::K_DEL, sync_key);
	}
	if(owner->ssdb->new_version(Bytes(sync_key), sync_type, &sync_version) == -1) {
		log_error("new version failed, key: %s, type: %c", hexmem(sync_key.data(), sync_key.size()).c_str(), sync_type);
		return -1;
	}
	return 0;
}

int RangeMigrate::Server::proc_req(const std::vector<Bytes> &req) {
	LogEvent log;
	if(log.load(req[0]) != 0) {
		log_error("invalid binlog!");
		return -1;
	}
	/* check arguments' number */
	if(req.size() != 2) {
		log_error("invalidate number of arguments");
		return -1;
	}

	switch(log.cmd()) {
		case BinlogCommand::ACK:
			{
				log_debug("ack received");
				first = true;
				/* set ttl */
				log_debug("recvieve ttl, key: %s ttl: %" PRId64, hexmem(sync_key.data(), sync_key.size()).c_str(), log.ttl());
				int64_t ttl = log.ttl();
				if(ttl >= 0) {
					if(owner->expiration->set_ttl(sync_key, ttl) == -1) {
						log_error("set ttl failed");
					} else {
						owner->binlog->write(BinlogType::SYNC, BinlogCommand::K_EXPIRE, sync_key, ttl);
					}
				}

				if(send_cmd(BinlogType::COPY, BinlogCommand::ACK, "", "range_sync_ack") != 0) {
					log_error("send command ack failed");
					return -1;
				}
			}
			break;
		case BinlogCommand::END:
			{
				log_debug("eof received");
				if(send_cmd(BinlogType::COPY, BinlogCommand::END, "", "range_sync_end") != 0) {
					log_error("send command eof failed");
					return -1;
				}
				// TODO enable slot
			}
			return 1;
		case BinlogCommand::KSET:
			{
				log_debug("kv received");
				sync_key = log.key().String();
				sync_type = DataType::KV;
				Transaction trans(owner->ssdb, sync_key);
				if(init_meta(trans) != 0) {
					return -1;
				}
				if(owner->ssdb->set(sync_key, req[1], trans, sync_version) == -1) {
					log_error("set %s failed", hexmem(sync_key.data(), sync_key.size()).c_str());
					return -1;
				}
				if (owner->binlog) {
					owner->binlog->write(BinlogType::SYNC, BinlogCommand::K_SET, log.key(), req[1]);
				}
			}
			break;
		case BinlogCommand::HSET:
			{
				log_debug("hash received");
				std::string field;
				if(decode_hash_key_ex(log.key(), &sync_key, &field) == -1) {
					log_error("decode hash key: %s failed",
						hexmem(sync_key.data(), sync_key.size()).c_str());
					return -1;
				}
				Transaction trans(owner->ssdb, sync_key);
				if(first) {
					sync_type = DataType::HASH;
					if(init_meta(trans) != 0) {
						return -1;
					}
					first = false;
				}
				if(owner->ssdb->hset(sync_key, field, req[1], trans, sync_version) == -1) {
					log_error("hset %s failed",
						hexmem(sync_key.data(), sync_key.size()).c_str());
					return -1;
				}
				if (owner->binlog) {
					owner->binlog->write(BinlogType::SYNC, BinlogCommand::H_SET, log.key(), req[1]);
				}
			}
			break;
		case BinlogCommand::ZSET:
			{
				log_debug("zset received");
				std::string field;
				if(decode_zset_key_ex(log.key(), &sync_key, &field) == -1) {
					log_error("decode zset key: %s failed",
						hexmem(sync_key.data(), sync_key.size()).c_str());
					return -1;
				}
				Transaction trans(owner->ssdb, sync_key);
				if(first) {
					sync_type = DataType::ZSET;
					if(init_meta(trans) != 0) {
						return -1;
					}
					first = false;
				}
				if(owner->ssdb->zset(sync_key, field, req[1], trans, sync_version) == -1) {
					log_error("zset %s failed",
						hexmem(sync_key.data(), sync_key.size()).c_str());
					return -1;
				}
				if (owner->binlog) {
					owner->binlog->write(BinlogType::SYNC, BinlogCommand::Z_SET, log.key(), req[1]);
				}
			}
			break;
		case BinlogCommand::SSET:
			{
				log_debug("set received");
				std::string sync_key = log.key().String();
				Transaction trans(owner->ssdb, sync_key);
				if(first) {
					sync_type = DataType::SET;
					if(init_meta(trans) == -1) {
						return -1;
					}
					first = false;
				}
				if(owner->ssdb->sset(sync_key, req[1], trans, sync_version) == -1) {
					log_error("set %s failed",
						hexmem(sync_key.data(), sync_key.size()).c_str());
					return -1;
				}
				if (owner->binlog) {
					owner->binlog->write(BinlogType::SYNC, BinlogCommand::S_SET, log.key(), req[1]);
				}
			}
			break;
		case BinlogCommand::QPUSH_BACK:
			{
				log_debug("queue received");
				sync_key = log.key().String();
				Transaction trans(owner->ssdb, sync_key);
				if(first) {
					sync_type = DataType::QUEUE;
					if(init_meta(trans) != 0) {
						return -1;
					}
					first = false;
				}
				if(owner->ssdb->qpush_back(sync_key, req[1], trans, sync_version) == -1) {
					log_error("qpush_back %s failed",
						hexmem(sync_key.data(), sync_key.size()).c_str());
					return -1;
				}
				if (owner->binlog) {
					owner->binlog->write(BinlogType::SYNC, BinlogCommand::Q_PUSH_BACK, log.key(), req[1]);
				}
			}
			break;
		default:
			log_error("invalidate cmd: %c", log.cmd());
			return -1;

	}
	return 0;
}

int RangeMigrate::Server::send_cmd(char log_type, char cmd, const Bytes &key, const Bytes &value) {
	LogEvent log(RANGE_MIGRATE_SEQ, log_type, cmd, Bytes(key));
    link->noblock();
	link->send(log.repr(), value);
	if(link->flush() == -1) {
		return -1;
	}
	return 0;
}

static const std::vector<Bytes> *sync_read(Fdevents *evb, Link *link, int64_t timeout_ms) {
	const std::vector<Bytes> *req = NULL;
	const Fdevents::events_t *events = NULL;
	int64_t remaining = timeout_ms;
	int64_t stime = time_ms();
	link->noblock();
	evb->set(link->fd(), FDEVENT_IN, 0, NULL);
	while(true) {
		/**
		 * As one read would generate multi requests,
		 * we always try parsing first. Timeout isn't taken
		 * into consideration at this moment.
		 **/
		req = link->recv();
		if(!req->empty()) {
			evb->del(link->fd());
			return req;
		}

		int64_t interval = (remaining > RANGE_MIGRATE_SYNCIO_RESOLUTION) ?
							remaining : RANGE_MIGRATE_SYNCIO_RESOLUTION;
		events = evb->wait(interval);
		if(events != NULL || !events->empty()) {
			if(link->read() < 0) {
				/* maybe the last request */
				req = link->recv();
				if(req->empty()) {
					evb->del(link->fd());
					return NULL;
				} else {
					log_debug("last migrate request");
					return req;
				}
			}
		}
		int elapsed = time_ms() - stime;
		if(elapsed > timeout_ms) {
			evb->del(link->fd());
			errno = ETIMEDOUT;
			return NULL;
		}
		remaining = timeout_ms - elapsed;
	}
}

