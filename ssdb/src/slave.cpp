/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "net/fde.h"
#include "util/log.h"
#include "slave.h"
#include "include.h"
#include "util/thread.h"
#include "serv.h"

/*
Slave::Slave(SSDB *ssdb, SSDB *meta, const char *ip, int port, bool is_mirror){
	this->quit_thread = false;
	this->ssdb = ssdb;
	this->meta = meta;
	this->status = DISCONNECTED;
	this->master_ip = std::string(ip);
	this->master_port = port;
	this->is_mirror = is_mirror;
	if(this->is_mirror){
		this->log_type = BinlogType::MIRROR;
	}else{
		this->log_type = BinlogType::SYNC;
	}

	{
		char buf[128];
		snprintf(buf, sizeof(buf), "%s|%d", master_ip.c_str(), master_port);
		this->set_id(buf);
	}

	this->link = NULL;
	this->last_seq = 0;
	this->last_key = "";
	this->connect_retry = 0;

	this->copy_count = 0;
	this->sync_count = 0;

	this->mi = NULL;
} */

Slave::Slave(SSDBServer *serv, SSDB *meta, int port) {
	this->serv = serv;
	this->meta = meta;
	this->server_port = port;
	this->status = DISCONNECTED;
	this->log_type = BinlogType::SYNC;
	this->link = NULL;
	this->quit_thread = false;
	this->running = false;
	this->mi = new MasterInfo(new RplInfoHandler(this->meta));

	this->connect_retry = 0;
	// statistics
	this->copy_count = 0;
	this->sync_count = 0;
	this->failover_seq = 0;
}

Slave::~Slave(){
	log_debug("stopping slave thread...");
	if (this->running) {
		this->stop();
	}
	if(link){
		delete link;
	}
	if (this->mi) {
		delete this->mi;
	}
	log_debug("Slave finalized");
}

std::string Slave::stats() const{
	std::string s;
	if (!this->running) {
		return "";
	}
	s.append("master_host:" + this->mi->ip + "\n");
	s.append("master_port:" + str(this->mi->port) + "\n");

	s.append("status:");
	switch(status){
	case DISCONNECTED:
		s.append("DISCONNECTED\n");
		break;
	case INIT:
		s.append("INIT\n");
		break;
	case COPY:
		s.append("COPY\n");
		break;
	case SYNC:
		s.append("SYNC\n");
		break;
	}

	s.append("last_seq:" + str(this->mi->last_seq) + "\n");
	s.append("last_key:" + this->mi->last_key + "\n");
	s.append("copy_count:" + str(copy_count) + "\n");
	s.append("sync_count:" + str(sync_count));
	return s;
}

void Slave::init() {
	this->mi->load();
}

void Slave::start(){
	if (this->running) {
		log_warn("slave thread is running");
		return;
	}

	log_debug("last_seq=%" PRIu64 ", last_key=%s",
			this->mi->last_seq, hexmem(mi->last_key.data(), mi->last_key.size()).c_str());

	this->quit_thread = false;
	this->running = true;
	int err = pthread_create(&this->thread_id, NULL, &Slave::_run_thread, this);
	if (err != 0) {
		log_error("can't create thread: %s", strerror(err));
		this->running = false;
	}
	/* disbale write */
	serv->global_read_lock = true;
}

void Slave::stop(){
	if (!this->running) {
		log_warn("slave thread not running!");
		return;
	}

	this->quit_thread = true;
	void *tret;
	int err = pthread_join(this->thread_id, &tret);
    if(err != 0){
		log_error("can't join thread: %s", strerror(err));
	}
}

void Slave::migrate_old_status(){
}

void Slave::load_status(){
}

void Slave::save_progress(bool include_last_key) {
	this->mi->save_last_seq();
	if (include_last_key) {
		this->mi->save_last_key();
	}
}

int Slave::connect(){
	const char *ip = this->mi->ip.c_str();
	int port = this->mi->port;
	this->version = 0;

	{
		if (connect_retry++ % 50 == 1) {
			log_info("connect retry %d connecting to master at %s:%d...", connect_retry, ip, port);
		}

		this->link = Link::connect(ip, port);

		if (this->link == NULL) {
			log_error("failed to connect to master: %s:%d %s", ip, port, strerror(errno));
			goto err;
		} else {
			this->status = INIT;
			connect_retry = 0;
		//	const char *type = is_mirror? "mirror" : "sync";

			//if(!this->auth.empty()){
			if (!this->mi->auth.empty()) {
				const std::vector<Bytes> *resp;
				//resp = link->request("auth", this->auth);
				resp = link->request("auth", this->mi->auth);
				if (resp->empty() || resp->at(0) != "ok") {
					log_error("auth error");
					delete link;
					link = NULL;
					sleep(1);
					return -1;
				}
			}

			log_info("send sync, last_seq=%" PRIu64 ", last_key=%s", this->mi->last_seq, this->mi->last_key.c_str());
			link->send("sync140", str(this->mi->last_seq), this->mi->last_key, str(this->server_port));
			if (link->flush() == -1) {
				log_error("network error");
				delete link;
				link = NULL;
				goto err;
			}
			return 1;
		}
	}
	return 0;
err:
	return -1;
}

void* Slave::_run_thread(void *arg){
	SET_PROC_NAME("replicator");
	Slave *slave = (Slave *)arg;
	const std::vector<Bytes> *req;
	Fdevents select;
	const Fdevents::events_t *events;
	int idle = 0;
	bool reconnect = false;

#define RECV_TIMEOUT		200
#define MAX_RECV_TIMEOUT	300 * 1000
#define MAX_RECV_IDLE		(MAX_RECV_TIMEOUT/RECV_TIMEOUT)

	while(true){
		if(slave->quit_thread) {
			if(slave->failover_seq <= 0 || slave->failover_seq == slave->mi->last_seq) {
				break;
			}
		}
		if(reconnect){
			slave->status = DISCONNECTED;
			reconnect = false;
			select.del(slave->link->fd());
			delete slave->link;
			slave->link = NULL;
			sleep(1);
		}
		if(!slave->connected()){
			if(slave->connect() != 1){
				usleep(100 * 1000);
			}else{
				select.set(slave->link->fd(), FDEVENT_IN, 0, NULL);
			}
			continue;
		}

		events = select.wait(RECV_TIMEOUT);
		if (events == NULL) {
			log_error("events.wait error: %s", strerror(errno));
			sleep(1);
			continue;
		} else if (events->empty()) {
			if (idle++ >= MAX_RECV_IDLE) {
				log_error("the master hasn't responsed for awhile, reconnect...");
				idle = 0;
				reconnect = true;
			}
			continue;
		}
		idle = 0;

		if (slave->link->read() <= 0) {
			log_error("link.read error: %s, reconnecting to master", strerror(errno));
			reconnect = true;
			continue;
		}

		while (1) {
			req = slave->link->recv();
			if (req == NULL) {
				log_error("link.recv error: %s, reconnecting to master", strerror(errno));
				reconnect = true;
				break;
			} else if (req->empty()) {
				break;
			} else if (req->at(0) == "noauth") {
				log_error("authentication required");
				reconnect = true;
				sleep(1);
				break;
			} else {
				if (slave->proc(*req) < 0) {
					log_error("slave proc failed.");
					/* goto err; */
					/* try to reconnect to fix error instead */
					reconnect = true;
					break;
				}
			}
		} // end while (1)
	} // end while (!this->quit-thread)

	slave->running = false;
	slave->failover_seq = 0;
	log_info("Slave thread quit, last_seq=%" PRIu64", last_key=%s", slave->mi->last_seq, slave->mi->last_key.c_str());

	if (slave->link) {
		select.del(slave->link->fd());
		delete slave->link;
		slave->link = NULL;
	}

	return (void *)NULL;
}

int Slave::proc(const std::vector<Bytes> &req) {
	//LogEventV1 event_v1;
	//LogEvent event_v2;

	/* check binlog version */
	/*if (version == 0) {
		if (event_v2.load(req[0]) < 0) {
			if (event_v1.load(req[0]) < 0) {
				goto failed;
			}
			version = 1;
			goto v1;
		}
		version = 2;
		goto v2;
	}

	if (version == 1) {
		if (event_v1.load(req[0]) < 0) {
			goto failed;
		}
		goto v1;
	}

	if (version == 2) {
		if (event_v2.load(req[0]) < 0) {
			goto failed;
		}
		goto v2;
	}

failed:
	log_error("invalid binlog event! version=%d, raw=%s", version, hexmem(req[0].data(), req[0].size()).c_str());
	return -1;

v1:
	return proc(event_v1, req);
v2:
	return proc(event_v2, req);*/
	LogEvent event;
	if(event.load(req[0]) < 0) {
		log_error("invalid binlog event! raw=%s", hexmem(req[0].data(), req[0].size()).c_str());
		return -1;
	}
	return proc(event, req);
}

int Slave::proc(const LogEvent &event, const std::vector<Bytes> &req){
	switch(event.type()){
		case BinlogType::NOOP:
			return this->proc_noop(event, req);
			break;
		case BinlogType::COPY:{
			status = COPY;
			log_debug("slave proc [copy] key[%s] seq[%" PRIu64 "]",
					hexmem(event.key().data(), event.key().size()).c_str(), event.seq());
			return this->proc_copy(event, req);
			break;
		}
		case BinlogType::SYNC:
		case BinlogType::MIRROR:{
			status = SYNC;
			if(++sync_count % 1000 == 1){
				log_info("sync_count=%" PRIu64 ", last_seq=%" PRIu64 ", seq=%" PRIu64,
					sync_count, this->mi->last_seq, event.seq());
			}
			log_debug("slave proc [sync] key[%s] seq[%lu]", event.key().data(), event.seq());
			return this->proc_sync(event, req);
			break;
		}
		default:
			log_warn("unknown event type: %d", (unsigned char)event.type());
			break;
	}
	return 0;
}

int Slave::proc_noop(uint64_t seq) {
	log_debug("noop last_seq=%" PRIu64 ", seq=%" PRIu64, this->mi->last_seq, seq);
	if (this->mi->last_seq != seq) {
		this->mi->last_seq = seq;
		this->save_progress(true);
	}
	return 0;
}

int Slave::proc_noop(const LogEvent &event, const std::vector<Bytes> &req){
	return proc_noop(event.seq());
}

int Slave::proc_copy(const LogEvent &event, const std::vector<Bytes> &req) {
	switch(event.cmd()){
		case BinlogCommand::BEGIN:
			/* disbale workers */
			log_info("full sync detected, disable server...");
			serv->net->pause();

			/* stop all backend sync threads */
			log_info("reset sync ...");
			serv->backend_sync->reset();

			log_info("flushdb...");
			/* flush db */
			serv->ssdb->flushdb();

			log_info("reset binlog...");
			/* reset binlog */
			if (serv->binlog->reset() != 0) {
				log_error("reset binlog failed");
				return -1;
			}

			/* reset replication info */
			log_info("reset replication info...");
			this->mi->last_seq = 0;
			this->mi->last_key = "";
			this->save_progress(true);

			/* reset slot info */
			log_info("reset slot ...");
			serv->ssdb_cluster->reset_slot();

			/* enable workers */
			log_info("enable server...");
			serv->net->proceed();

			log_info("copy begin");
			break;
		case BinlogCommand::END:
			log_info("copy end, copy_count=%" PRIu64 ", last_seq=%" PRIu64 ", seq=%" PRIu64,
				copy_count, this->mi->last_seq, event.seq());
			this->status = SYNC;
			this->mi->last_seq = event.seq();
			this->mi->last_key = "";
			this->save_progress(true);
			break;
		default:
			if(++copy_count % 1000 == 1){
				log_debug("copy_count=%" PRIu64 ", last_seq=%" PRIu64 ", seq=%" PRIu64 "",
					copy_count, this->mi->last_seq, event.seq());
			}
			return proc_sync(event, req);
			break;
	}
	return 0;
}

int Slave::proc_sync(const LogEvent &event, const std::vector<Bytes> &req){

	int ret = 0;
	switch(event.cmd()){

	// KV
	case BinlogCommand::K_SET:
		ret = proc_k_set(event);
		break;
	case BinlogCommand::K_DEL:
		ret = proc_k_del(event);
		break;
	case BinlogCommand::K_INCR:
		ret = proc_k_incr(event);
		break;
	case BinlogCommand::K_DECR:
		ret = proc_k_decr(event);
		break;
	case BinlogCommand::K_EXPIRE:
		ret = proc_k_expire(event);
		break;
	case BinlogCommand::K_EXPIRE_AT:
		ret = proc_k_expire_at(event);
		break;
	case BinlogCommand::K_SETBIT:
		ret = proc_k_setbit(event);
		break;

	// HASH
	case BinlogCommand::H_SET:
		ret = proc_h_set(event);
		break;
	case BinlogCommand::H_DEL:
		ret = proc_h_del(event);
		break;
	case BinlogCommand::H_CLEAR:
		ret = proc_h_clear(event);
		break;
	case BinlogCommand::H_INCR:
		ret = proc_h_incr(event);
		break;
	case BinlogCommand::H_DECR:
		ret = proc_h_decr(event);
		break;

	// ZSET
	case BinlogCommand::Z_SET:
		ret = proc_z_set(event);
		break;
	case BinlogCommand::Z_DEL:
		ret = proc_z_del(event);
		break;
	case BinlogCommand::Z_CLEAR:
		ret = proc_z_clear(event);
		break;
	case BinlogCommand::Z_INCR:
		ret = proc_z_incr(event);
		break;
	case BinlogCommand::Z_DECR:
		ret = proc_z_decr(event);
		break;
	case BinlogCommand::Z_POP_FRONT:
		ret = proc_z_pop_front(event);
		break;
	case BinlogCommand::Z_POP_BACK:
		ret = proc_z_pop_back(event);
		break;

	// QUEUE
	case BinlogCommand::Q_PUSH_FRONT:
		ret = proc_q_push_front(event);
		break;
	case BinlogCommand::Q_PUSH_BACK:
		ret = proc_q_push_back(event);
		break;
	case BinlogCommand::Q_POP_FRONT:
		ret = proc_q_pop_front(event);
		break;
	case BinlogCommand::Q_POP_BACK:
		ret = proc_q_pop_back(event);
		break;
	case BinlogCommand::Q_FIX:
		ret = proc_q_fix(event);
		break;
	case BinlogCommand::Q_CLEAR:
		ret = proc_q_clear(event);
		break;
	case BinlogCommand::Q_SET:
		ret = proc_q_set(event);
		break;

	// SET
	case BinlogCommand::S_SET:
		ret = proc_s_set(event);
		break;
	case BinlogCommand::S_DEL:
		ret = proc_s_del(event);
		break;
	case BinlogCommand::S_CLEAR:
		ret = proc_s_clear(event);
		break;
	case BinlogCommand::RAW:
		ret = proc_raw(event);
		break;

	case BinlogCommand::STOP:
	case BinlogCommand::DESC:
	case BinlogCommand::ROTATE:
		log_debug("skip invalidate binlog, type=%" PRId8 ", cmd=%" PRIu8, event.type(), (unsigned char)event.cmd());
		break;

	default:
		log_error("unknown binlog, type=%" PRId8 ", cmd=%" PRIu8, event.type(), (unsigned char)event.cmd());
		ret = -1;
		break;
	}

	if (ret < 0) {
		log_error("apply binlog event failed seq=%" PRIu64 ", cmd=%" PRIu8, event.seq(), (unsigned char)event.cmd());
		return ret;
	}

	this->mi->last_seq = event.seq();
	if(event.type() == BinlogType::COPY){
		this->mi->last_key = event.key().String();
	}
	this->save_progress(event.type() == BinlogType::COPY);

	return 0;
}

int Slave::proc_raw(const LogEvent &event) {
	/* as it is must in copy, there is no need to write binlog */
	Bytes key = event.key();
	Bytes val = event.val();
	log_debug("proc raw, key: %s value: %s",
			hexmem(key.data(), key.size()).c_str(), hexmem(val.data(), val.size()).c_str());
	return serv->ssdb->raw_set(event.key(), event.val());
}

#define SLAVE_PROC_CHECK_VERSION(key) \
do { \
	int ret = serv->ssdb->get_version(key, &t, &version); \
	if(ret == -1) { \
		return -1; \
	} \
	if(ret == 0) { \
		if(serv->ssdb->new_version(key, t, &version) == -1) { \
			return -1; \
		} \
	} \
} while(0)

int Slave::proc_k_set(const LogEvent &event) {
	assert(event.cmd() == BinlogCommand::K_SET);

	Bytes key = event.key();
	Bytes val = event.val();

	uint64_t version;
	char t = DataType::KV;
	SLAVE_PROC_CHECK_VERSION(key);
	if(t != DataType::KV) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::KV));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->set(key, val, trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_SET,
				key, val);
	}

	return ret;
}

int Slave::proc_k_del(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::K_DEL);

	Bytes key = event.key();

	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->del(key, trans);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_DEL, key);
	}

	serv->expiration->del_ttl(key);

	return ret;
}

int Slave::proc_k_incr(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::K_INCR);

	Bytes key = event.key();
	Bytes val = event.val();
	assert (val.size() == sizeof(int64_t));

	int64_t by = (int64_t)decode_uint64(*((uint64_t*)val.data()));

	uint64_t version;
	char t = DataType::KV;
	SLAVE_PROC_CHECK_VERSION(key);
	if(t != DataType::KV) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::KV));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int64_t new_val;
	int ret = serv->ssdb->incr(key, by, &new_val, trans, version);
	if (ret > 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_INCR,
				key, val);
	}

	return ret;
}

int Slave::proc_k_decr(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::K_DECR);

	Bytes key = event.key();
	Bytes val = event.val();
	assert (val.size() == sizeof(int64_t));

	int64_t by = (int64_t)decode_uint64(*((uint64_t*)val.data()));
	int64_t dir = -1;

	uint64_t version;
	char t = DataType::KV;
	SLAVE_PROC_CHECK_VERSION(key);
	if(t != DataType::KV) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::KV));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int64_t new_val;
	int ret = serv->ssdb->incr(key, dir*by, &new_val, trans, version);
	if (ret > 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_DECR,
				key, val);
	}
	return ret;
}

int Slave::proc_k_expire(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::K_EXPIRE);

	Bytes key = event.key();
	Bytes val = event.val();

	Transaction trans(serv->ssdb, key);
	uint64_t version;
	char t;
	int ret = serv->ssdb->get_version(key, &t, &version);
	if (ret == 1) {
		ret = serv->expiration->set_ttl(key, val.Int());
		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_EXPIRE,
					key, val);
		}
	}

	return ret;
}

int Slave::proc_k_expire_at(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::K_EXPIRE_AT);

	Bytes key = event.key();
	Bytes val = event.val();

	Transaction trans(serv->ssdb, key);
	uint64_t version;
	char t;
	int ret = serv->ssdb->get_version(key, &t, &version);
	if (ret == 1) {
		int64_t span = val.Int64()-time(NULL);
		ret = serv->expiration->set_ttl(key, span);
		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_EXPIRE_AT,
					key, val);
		}
	}

	return ret;
}

int Slave::proc_k_setbit(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::K_SETBIT);

	Bytes key = event.key();
	Bytes val = event.val();

	assert (val.size() == sizeof(uint64_t)*2);
	int64_t offset = (int64_t)decode_uint64(*((uint64_t *)val.data()));
	int64_t on = (int64_t)decode_uint64(*((uint64_t *)(val.data()+sizeof(uint64_t))));

	uint64_t version;
	char t = DataType::KV;
	SLAVE_PROC_CHECK_VERSION(key);
	if(t != DataType::KV) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::KV));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->setbit(key, offset, on, trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_SETBIT,
				key, val);
	}

	return ret;
}

int Slave::proc_h_set(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::H_SET);

	Bytes key = event.key();
	Bytes val = event.val();

	std::string k, f;
	uint64_t version;
	int ret = decode_hash_key_ex(key, &k, &f);
	if (ret < 0) {
		log_error("slave decode hash key failed, type=%" PRId8 "seq=%" PRIu64, event.type(), event.seq());
		return ret;
	}

	char t = DataType::HASH;
	SLAVE_PROC_CHECK_VERSION(k);
	if (t != DataType::HASH) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::HASH));
		return -1;
	}

	Transaction trans(serv->ssdb, k);
	ret = serv->ssdb->hset(Bytes(k.data(), k.size()),
			Bytes(f.data(), f.size()), val, trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::H_SET, key, val);
	}

	return ret;
}

int Slave::proc_h_del(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::H_DEL);

	Bytes key = event.key();
	Bytes field = event.val();

	uint64_t version;
	char t = DataType::HASH;
	SLAVE_PROC_CHECK_VERSION(key);
	if(t != DataType::HASH) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::HASH));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->hdel(Bytes(key.data(), key.size()),
			Bytes(field.data(), field.size()), trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::H_DEL, key, field);
	}

	return ret;
}

int Slave::proc_h_clear(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::H_CLEAR);

	Bytes key = event.key();

	uint64_t version;
	char t = DataType::HASH;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::HASH) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::HASH));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int64_t count = serv->ssdb->hclear(key, trans, version);
	if (count >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::H_CLEAR, key);
	}

	return count >= 0 ? 0 : -1;
}

int Slave::proc_h_incr(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::H_INCR);

	Bytes key = event.key();
	Bytes val = event.val();

	std::string k, f;
	uint64_t version;
	int ret = decode_hash_key_ex(key, &k, &f);
	if (ret < 0) {
		log_error("slave decode hash key failed, type=%" PRId8 " seq=%" PRIu64, event.type(), event.seq());
		return ret;
	}

	assert (val.size() == sizeof(uint64_t));
	int64_t by = (int64_t)decode_uint64(*((uint64_t *)val.data()));

	char t = DataType::HASH;
	SLAVE_PROC_CHECK_VERSION(k);
	if (t != DataType::HASH) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::HASH));
		return -1;
	}

	Transaction trans(serv->ssdb, k);
	int64_t new_val;
	ret = serv->ssdb->hincr(Bytes(k.data(), k.size()),
			Bytes(f.data(), f.size()), by, &new_val, trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::H_INCR, key, val);
	}

	return ret;
}

int Slave::proc_h_decr(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::H_DECR);

	Bytes key = event.key();
	Bytes val = event.val();

	std::string k, f;
	uint64_t version;
	int ret = decode_hash_key_ex(key, &k, &f);
	if (ret < 0) {
		log_error("decode hash key failed, type=%" PRId8 " seq=%" PRIu64, event.type(), event.seq());
		return ret;
	}

	assert (val.size() == sizeof(uint64_t));
	int64_t by = (int64_t)decode_uint64(*((uint64_t *)val.data()));
	int64_t dir = -1;

	char t;
	SLAVE_PROC_CHECK_VERSION(k);
	if (t != DataType::HASH) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::HASH));
		return -1;
	}

	Transaction trans(serv->ssdb, k);
	int64_t new_val;
	ret = serv->ssdb->hincr(Bytes(k.data(), k.size()),
			Bytes(f.data(), f.size()), dir*by, &new_val, trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::H_DECR,
				key, val);
	}

	return ret;
}

int Slave::proc_z_set(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Z_SET);

	Bytes key = event.key();
	Bytes val = event.val();

	std::string k, f;
	uint64_t version;
	int ret = decode_zset_key_ex(key, &k, &f);
	if (ret < 0) {
		log_error("decode zset key failed, type=%" PRId8 " seq=%" PRIu64, event.type(), event.seq());
		return ret;
	}

	char t = DataType::ZSET;
	SLAVE_PROC_CHECK_VERSION(k);
	if (t != DataType::ZSET) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::ZSET));
		return -1;
	}

	Transaction trans(serv->ssdb, k);
	ret = serv->ssdb->zset(Bytes(k.data(), k.size()),
			Bytes(f.data(), f.size()), val, trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_SET, key, val);
	}

	return ret;
}

int Slave::proc_z_del(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Z_DEL);

	Bytes key = event.key();
	Bytes field = event.val();

	uint64_t version;
	char t = DataType::ZSET;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::ZSET) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::ZSET));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->zdel(key, field, trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_DEL, key, field);
	}

	return ret;
}

int Slave::proc_z_clear(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Z_CLEAR);

	Bytes key = event.key();

	uint64_t offset = 0;
	uint64_t limit = 1000;
	int ret = 0;

	uint64_t version;
	char t = DataType::ZSET;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::ZSET) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::ZSET));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	while (1) {
		ZIterator *it = serv->ssdb->zrange(key, offset, limit, version);
		int num = 0;
		while (it->next()) {
			ret = serv->ssdb->zdel(key, it->field, trans, version);
			if (ret < 0) {
				log_error("zclear zdel failed, type=%" PRId8 " seq=%" PRIu64, event.type(), event.seq());
				num = 0;
				break;
			}
			num++;
		}
		delete it;

		if (num == 0) {
			break;
		}
	}

	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_CLEAR, key);
	}

	return ret;
}

int Slave::proc_z_incr(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Z_INCR);

	Bytes key = event.key();
	Bytes val = event.val();

	std::string k, f;
	uint64_t version;
	int ret = decode_zset_key_ex(key, &k, &f);
	if (ret < 0) {
		log_error("slave decode zset key failed, type=%" PRId8 ", seq=%" PRIu64, event.type(), event.seq());
		return ret;
	}

	assert (val.size() == sizeof(uint64_t));
	int64_t by = (int64_t)decode_uint64(*((uint64_t *)val.data()));
	int64_t new_val;

	char t = DataType::ZSET;
	SLAVE_PROC_CHECK_VERSION(k);
	if (t != DataType::ZSET) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::ZSET));
		return -1;
	}

	Transaction trans(serv->ssdb, k);
	ret = serv->ssdb->zincr(Bytes(k.data(), k.size()),
			Bytes(f.data(), f.size()), by, &new_val, trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_INCR,
				key, val);
	}

	return ret;
}

int Slave::proc_z_decr(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Z_DECR);

	Bytes key = event.key();
	Bytes val = event.val();

	std::string k, f;
	uint64_t version;
	int ret = decode_zset_key_ex(key, &k, &f);
	if (ret < 0) {
		log_error("slave decode zset key failed, type=%" PRId8 ", seq=%" PRIu64, event.type(), event.seq());
		return ret;
	}

	assert (val.size() == sizeof(uint64_t));
	int64_t by = (int64_t)decode_uint64(*((uint64_t *)val.data()));
	int64_t dir = -1;
	int64_t new_val;

	char t = DataType::ZSET;
	SLAVE_PROC_CHECK_VERSION(k);
	if (t != DataType::ZSET) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::ZSET));
		return -1;
	}

	Transaction trans(serv->ssdb, k);
	ret = serv->ssdb->zincr(Bytes(k.data(), k.size()),
			Bytes(f.data(), f.size()), dir*by, &new_val, trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_DECR,
				key, val);
	}

	return ret;
}

int Slave::zpop(ZIterator *it, SSDBServer *serv, const Bytes &key, Transaction &trans) {
	while (it->next()) {
		int ret = serv->ssdb->zdel(key, it->key, trans, it->version);
		if (ret < 0)
			return ret;
	}
	return 0;
}

int Slave::proc_z_pop_front(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Z_POP_FRONT);

	Bytes key = event.key();
	Bytes val = event.val();

	uint64_t version;
	char t = DataType::ZSET;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::ZSET) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::ZSET));
		return -1;
	}

	uint64_t limit = val.Uint64();
	Transaction trans(serv->ssdb, key);
	ZIterator *it = serv->ssdb->zscan(key, "", "", "", limit, version);
	int ret = zpop(it, serv, key, trans);
	delete it;

	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_POP_FRONT,
				key, val);
	}

	return ret;
}

int Slave::proc_z_pop_back(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Z_POP_BACK);

	Bytes key = event.key();
	Bytes val = event.val();

	uint64_t version;
	char t = DataType::ZSET;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::ZSET) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::ZSET));
		return -1;
	}

	uint64_t limit = val.Uint64();
	Transaction trans(serv->ssdb, key);
	ZIterator *it = serv->ssdb->zrscan(key, "", "", "", limit, version);
	int ret = zpop(it, serv, key, trans);
	delete it;

	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_POP_BACK,
				key, val);
	}

	return ret;
}

int Slave::proc_q_push_front(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Q_PUSH_FRONT);

	Bytes key = event.key();
	Bytes item = event.val();

	int64_t size = 0;
	uint64_t version;
	char t = DataType::QUEUE;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::QUEUE) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::QUEUE));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	size = serv->ssdb->qpush_front(key, item, trans, version);
	if (size >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_PUSH_FRONT,
				key, item);
	}

	return size>=0 ? 0 : -1;
}

int Slave::proc_q_push_back(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Q_PUSH_BACK);

	Bytes key = event.key();
	Bytes item = event.val();
	std::string q_key;
	int64_t size = 0;

	uint64_t version;
	char t = DataType::QUEUE;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::QUEUE) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::QUEUE));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	size = serv->ssdb->qpush_back(key, item, trans, version);
	if (size >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_PUSH_BACK,
				key, item);
	}

	return size>=0 ? 0 : -1;
}

int Slave::proc_q_pop_front(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Q_POP_FRONT);

	Bytes key = event.key();
	Bytes val = event.val();

	assert (val.size() == sizeof(uint64_t));
	uint64_t size = decode_uint64(*((uint64_t *)val.data()));

	uint64_t version;
	char t = DataType::QUEUE;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::QUEUE) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::QUEUE));
		return -1;
	}

	int ret = 0;
	Transaction trans(serv->ssdb, key);
	while (size-- > 0) {
		std::string item;
		ret = serv->ssdb->qpop_front(key, &item, trans, version);
		if (ret <= 0) {
			break;
		}
	}
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_POP_FRONT,
				key, val);
	}

	return ret;
}

int Slave::proc_q_pop_back(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Q_POP_BACK);

	Bytes key = event.key();
	Bytes val = event.val();

	assert (val.size() == sizeof(uint64_t));
	uint64_t size = decode_uint64(*((uint64_t *)val.data()));

	uint64_t version;
	char t = DataType::QUEUE;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::QUEUE) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::QUEUE));
		return -1;
	}

	int ret = 0;
	Transaction trans(serv->ssdb, key);
	while (size-- > 0) {
		std::string item;
		ret = serv->ssdb->qpop_back(key, &item, trans, version);
		if (ret <= 0) {
			break;
		}
	}
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_POP_BACK,
				key, val);
	}

	return ret;
}

int Slave::proc_q_fix(const LogEvent &event) {
	/*assert (event.cmd() == BinlogCommand::Q_FIX);

	Bytes key = event.key();

	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->qfix(key, trans);

	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_FIX, key);
	}

	return ret;*/
	return 0;
}

int Slave::proc_q_clear(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Q_CLEAR);

	Bytes key = event.key();

	uint64_t version;
	char t = DataType::QUEUE;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::QUEUE) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::QUEUE));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int ret = 0;
	while (1) {
		std::string item;
		ret = serv->ssdb->qpop_front(key, &item, trans, version);
		if (ret == 0) {
			break;
		}
		if (ret < 0) {
			break;
		}
	}
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_CLEAR, key);
	}

	return ret;
}

int Slave::proc_q_set(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::Q_SET);

	Bytes key = event.key();
	Bytes val = event.val();

	std::string k;
	uint64_t tmp;
	uint64_t version;
	int ret = decode_qitem_key_ex(key, &k, &tmp);
	if (ret < 0) {
		log_error("decode qitem key failed, type=%" PRId8 ", seq=%" PRIu64, event.type(), event.seq());
		return ret;
	}
	int64_t index = (int64_t)decode_uint64(tmp);

	char t = DataType::QUEUE;
	SLAVE_PROC_CHECK_VERSION(k);
	if (t != DataType::QUEUE) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::QUEUE));
		return -1;
	}

	Transaction trans(serv->ssdb, k);
	ret = serv->ssdb->qset(k, index, val, trans, version);
	if (ret > 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_SET, key, val);
	}

	return ret>0 ? 0 : -1;
}

int Slave::proc_s_set(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::S_SET);

	Bytes key = event.key();
	Bytes item = event.val();

	uint64_t version;
	char t = DataType::SET;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::SET) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::SET));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->sset(key, item, trans, version);
	if (ret > 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC,
				BinlogCommand::S_SET, key, item);
	}

	return ret;
}

int Slave::proc_s_del(const LogEvent &event) {
	assert (event.cmd() == BinlogCommand::S_DEL);

	Bytes key = event.key();
	Bytes item = event.val();

	uint64_t version;
	char t = DataType::SET;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::SET) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::SET));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->sdel(key, item, trans, version);
	if (ret > 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC,
			BinlogCommand::S_DEL, key, item);
	}

	return ret;
}

int Slave::proc_s_clear(const LogEvent &event) {
	assert(event.cmd() == BinlogCommand::S_CLEAR);

	Bytes key = event.key();

	uint64_t version;
	char t = DataType::SET;
	SLAVE_PROC_CHECK_VERSION(key);
	if (t != DataType::SET) {
		log_error("unexpected data type: %" PRIu8 " expected:%" PRIu8, uint8_t(t), uint8_t(DataType::SET));
		return -1;
	}

	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->sclear(key, trans, version);
	if (ret >0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC,
			BinlogCommand::S_CLEAR, key);
	}
	return ret;
}
