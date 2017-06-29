/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* kv */
#include <time.h>
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_get(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	std::string val;
	int ret = 0;
	if(exists) {
		ret = serv->ssdb->get(req[1], &val, version);
	}
	resp->reply_get(ret, &val);
	return 0;
}

int proc_getset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	Transaction trans(serv->ssdb, req[1]);
	/* get */
	std::string val;
	int gret = 0;
	if(exists) {
		gret = serv->ssdb->get(req[1], &val, version);
		if(gret < 0) {
			resp->push_back("error");
			resp->push_back("server inner error");
			return 0;
		}
	} else {
		NEW_VERSION(req[1], op, version);
	}

	/* set */
	int ret = serv->ssdb->set(req[1], req[2], trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_SET, req[1], req[2]);
	}

	resp->reply_get(gret, &val);
	return 0;
}

int proc_set(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}

	Transaction trans(serv->ssdb, req[1]);
	int ret = serv->ssdb->set(req[1], req[2], trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_SET,
				req[1], req[2]);
	}

	if(ret == -1){
		resp->push_back("error");
	}else{
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;
}

int proc_setnx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(exists) {
		resp->reply_bool(0);
		return 0;
	}

	NEW_VERSION(req[1], op, version);
	Transaction trans(serv->ssdb, req[1]);
	int ret = serv->ssdb->set(req[1], req[2], trans, version);
	if (ret > 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_SET,
				req[1], req[2]);
	}

	resp->reply_bool(ret);
	return 0;
}

int proc_setx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}

	Transaction trans(serv->ssdb, req[1]);
	int ret = serv->ssdb->set(req[1], req[2], trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_SET,
				req[1], req[2]);
	}
	if(ret == -1){
		resp->push_back("error");
		return 0;
	}

	ret = serv->expiration->set_ttl(req[1], req[3].Int());
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_EXPIRE,
				req[1], req[3]);
	}
	if(ret == -1){
		resp->push_back("error");
	}else{
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;
}

int proc_ttl(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if (!exists) {
		resp->push_back("-2");
		return 0;
	}

	int64_t ttl = serv->expiration->get_ttl(req[1]);
	resp->push_back(str(ttl));
	return 0;
}

int proc_expire(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	Transaction trans(serv->ssdb, req[1]);
	std::string val;
	if(exists){
		int ret = serv->expiration->set_ttl(req[1], req[2].Int64());
		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_EXPIRE,
					req[1], req[2]);
		}

		if(ret != -1){
			resp->push_back("ok");
			resp->push_back("1");
			return 0;
		}
	}
	resp->push_back("ok");
	resp->push_back("0");
	return 0;
}

int proc_expire_at(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	Transaction trans(serv->ssdb, req[1]);
	int64_t span = req[2].Int64() - time(NULL);
	std::string val;
	if(exists){
		int ret = serv->expiration->set_ttl(req[1], span);
		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_EXPIRE_AT,
					req[1], req[2]);
		}

		if(ret != -1){
			resp->push_back("ok");
			resp->push_back("1");
			return 0;
		}
	}
	resp->push_back("ok");
	resp->push_back("0");
	return 0;
}

int proc_pexpire(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	Transaction trans(serv->ssdb, req[1]);
	std::string val;
	int64_t ttl = req[2].Int64()/1000;
	if(exists){
		int ret = serv->expiration->set_ttl(req[1], ttl);
		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_EXPIRE,
					req[1], str(ttl));
		}

		if(ret != -1){
			resp->push_back("ok");
			resp->push_back("1");
			return 0;
		}
	}
	resp->push_back("ok");
	resp->push_back("0");
	return 0;
}

int proc_pexpire_at(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	Transaction trans(serv->ssdb, req[1]);
	int64_t ttl = req[2].Int64()/1000;
	int64_t span = ttl - time(NULL);
	std::string val;
	if(exists){
		int ret = serv->expiration->set_ttl(req[1], span);
		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_EXPIRE_AT,
					req[1], str(ttl));
		}

		if(ret != -1){
			resp->push_back("ok");
			resp->push_back("1");
			return 0;
		}
	}
	resp->push_back("ok");
	resp->push_back("0");
	return 0;
}

int proc_exists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->reply_bool(exists);
	return 0;
}

#define CHECK_CROSS_SLOT(req, resp, slot, step) \
do{ \
	slot = -1; \
	for(Request::const_iterator it = req.begin()+1; it != req.end(); it += (step)) { \
		int16_t s = KEY_HASH_SLOT((*it)); \
		if(slot == -1) { \
			slot = s; \
		} else if (slot != s) { \
			resp->clear(); \
			resp->push_back("error"); \
			resp->push_back("crossslot"); \
			return 0; \
		}\
	}\
} while(0)

#define CHECK_MULTI_ASKING(serv, link, req, resp, slot, step) \
do { \
	int flag = 0; \
	int ret = serv->ssdb_cluster->test_slot_importing(slot, &flag); \
	if(ret != 0) { \
		resp->clear(); \
		resp->push_back("error"); \
		resp->push_back("server get slot info failed"); \
		log_warn("test slot importing failed");	\
		return 0; \
	} \
	if(flag) { \
		if(link->asking) { \
			link->asking = false; \
			for(Request::const_iterator it = req.begin()+1; it != req.end(); it += (step)) { \
				uint64_t version; \
				char op; \
				int exists; \
				CHECK_META((*it), op, version, exists); \
				if(!exists) { \
					resp->clear(); \
					resp->push_back("error"); \
					resp->push_back("tryagain"); \
					return 0; \
				} \
			} \
			break; \
		} else { \
			resp->clear(); \
			resp->push_back("error"); \
			resp->push_back("slot migrating"); \
		}\
		return 0;\
	} \
} while(0)

#define CHECK_MULTI_ASK(serv, req, resp, slot, step) \
do { \
	if(migrating_slot == slot) { \
		for(Request::const_iterator it = req.begin()+1; it != req.end(); it+= (step)) { \
			CHECK_KEY(*it); \
			KeyLock &key_lock = serv->ssdb_cluster->get_key_lock((*it).String()); \
			if(key_lock.test_key((*it).String())) { \
				resp->clear(); \
				resp->push_back("error"); \
				resp->push_back("tryagain"); \
				return 0; \
			} \
			uint64_t version; \
			char op; \
			int exists; \
			CHECK_META((*it), op, version, exists); \
			if(!exists) { \
				resp->clear(); \
				resp->push_back("error"); \
				resp->push_back("ask"); \
				return 0; \
			} \
		} \
		goto action; \
	} \
} while(0)


int proc_multi_exists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer * serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	/* don't check crossslot */
	int16_t slot = -1;
	CHECK_CROSS_SLOT(req, resp, slot, 1);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));
	int16_t migrating_slot = 0;
	GET_SLOT_MIGRATING(serv, resp, migrating_slot);

	CHECK_MULTI_ASK(serv, req, resp, slot, 1);
	CHECK_MULTI_ASKING(serv, link, req, resp, slot, 1);

action:
	resp->push_back("ok");
	for(Request::const_iterator it = req.begin()+1; it != req.end(); it++) {
		uint64_t version;
		char op;
		int exists;
		CHECK_META((*it), op, version, exists);
		if(exists) {
			resp->push_back("1");
		} else {
			resp->push_back("0");
		}
	}
	return 0;
}

/*
 * ssdb mset is different from redis mset command:
 * 1)redis mset command awalys return OK ,it must success
 * 2)if ssdb mset is failed during a series of sets ,return try again
 *
 * if return try again ,maybe partially success ,other fail
 * Cautious ,it's a uncertain result
 */
int proc_multi_set(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("deprecated");
	return 0;
// 	SSDBServer *serv = (SSDBServer *)net->data;
// 	CHECK_NUM_PARAMS(3);
// 	CHECK_PARAMS_ODD(req);

// 	int16_t slot = -1;
// 	CHECK_CROSS_SLOT(req, resp, slot, 1);
// 	CHECK_SLOT_MOVED(slot);
// 	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

// 	int16_t migrating_slot = 0;
// 	GET_SLOT_MIGRATING(serv, resp, migrating_slot);
// 	CHECK_MULTI_ASK(serv, req, resp, slot, 1);
// 	CHECK_MULTI_ASKING(serv, link, req, resp, slot, 1);

// action:
// 	for(Request::const_iterator it = req.begin()+1; it != req.end(); it+=2) {
// 		const Bytes &key = *it;
// 		const Bytes &value = *(it + 1);
// 		char op;
// 		int exists;
// 		uint64_t version;
// 		CHECK_META(key, op, version, exists);
// 		CHECK_DATA_TYPE_KV(op);

// 		if(!exists) {
// 			NEW_VERSION(key, op, version);
// 		}

// 		Transaction trans(serv->ssdb, key);
// 		int ret = serv->ssdb->set(key, value, trans, version);
// 		if (ret >= 0 && serv->binlog) {
// 			serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_SET,
// 					key, value);
// 		}

// 		if(ret == -1){
// 			resp->push_back("error");
// 			resp->push_back("tryagain");
// 			return 0;
// 		}
// 	}

// 	resp->push_back("ok");
// 	resp->push_back("1");
// 	return 0;
}

int proc_multi_del(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(2);

	int16_t slot = -1;
	CHECK_CROSS_SLOT(req, resp, slot, 1);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));
	int16_t migrating_slot = 0;
	GET_SLOT_MIGRATING(serv, resp, migrating_slot);

	CHECK_MULTI_ASK(serv, req, resp, slot, 1);
	CHECK_MULTI_ASKING(serv, link, req, resp, slot, 1);

action:
	int count = 0;
	for(Request::const_iterator it = req.begin()+1; it != req.end(); it++) {
		const Bytes &key = *it;
		Transaction trans(serv->ssdb, key);
		int ret = serv->ssdb->del(key, trans);
		if(ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_DEL, key);
		}
		if(ret < 0) {
			resp->push_back("error");
			resp->push_back("delete failed");
			return 0;
		}
		serv->expiration->del_ttl(key);
		if(ret > 0) {
			count++;
		}
	}
	resp->reply_int(0, count);
	return 0;
}

int proc_multi_get(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	serv->ssdb->lock_db();
	const leveldb::Snapshot *snapshot = serv->ssdb->get_snapshot();
	serv->ssdb->unlock_db();

	int ret;
	std::vector<Bytes> key_list;
	std::vector<std::string> val_list;
	std::vector<uint64_t> version_list;
	for(Request::const_iterator it = req.begin()+1; it != req.end(); it++) {
		const Bytes &key = *it;
		int flag = 0;
		char op;
		int exists;
		uint64_t version;
		int16_t migrating_slot = -1;
		int16_t slot = KEY_HASH_SLOT(key);

		/* meta data*/
		exists = serv->ssdb->get_version(key, &op, &version, snapshot);
		if(exists == -1) {
			resp->clear();
			resp->push_back("error");
			resp->push_back("server inner error");
			goto exception;
		}
		if(op != DataType::KV && exists) {
			resp->clear();
			resp->push_back("error");
			resp->push_back("WRONGTYPE Operation against a key holding the wrong kind of value");
			log_error("WRONGTYPE Operation against a key holding the wrong kind of value");
			goto exception;
		}

		/*
		 * node is source
		 */
		ret = serv->ssdb_cluster->get_slot_migrating(&migrating_slot);
		if(ret != 0) {
			resp->clear();
			resp->push_back("error");
			resp->push_back("server inner error");
			goto exception;
		}

		if(!exists && migrating_slot == slot) {
			resp->clear();
			resp->push_back("error");
			resp->push_back("ask");
			goto exception;
		}

		/*
		 * node is target
		 */
		ret = serv->ssdb_cluster->test_slot_importing(slot, &flag); \
		if(ret != 0) {
			resp->clear();
			resp->push_back("error");
			resp->push_back("server get slot info failed");
			log_warn("test slot importing failed");
			goto exception;
		}
		if(flag) {
			if(link->asking) {
				link->asking = false;
			} else {
				resp->clear();
				resp->push_back("error");
				resp->push_back("slot migrating");
				goto exception;
			}
		}

		key_list.push_back(*it);
		version_list.push_back(version);
	}

	/* action */
	ret = serv->ssdb->mget(key_list, &val_list, version_list, snapshot);
	if (ret == 0) {
		resp->clear();
		resp->push_back("error");
		resp->push_back("params error");
		goto exception;
	} else if (ret == -1) {
		resp->clear();
		resp->push_back("error");
		resp->push_back("server inner error");
		goto exception;
	}
	resp->reply_list(0, val_list);

exception:
	serv->ssdb->release_snapshot(snapshot);
	return 0;
}

int proc_del(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->push_back("ok");
		resp->push_back("0");
		return 0;
	}
	Transaction trans(serv->ssdb, req[1]);
	int ret = serv->ssdb->del(req[1], trans);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_DEL, req[1]);
	}
	if(ret == -1){
		resp->push_back("error");
		resp->push_back("delete failed");
	}else{
		serv->expiration->del_ttl(req[1]);
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;
}

int proc_scan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("deprecated");
	return 0;
}

int proc_rscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("deprecated");
	return 0;
}

int proc_keys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("TODO");
	return 0;
}

int proc_rkeys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("deprecated");
	return 0;
}

// dir := +1|-1
static int _incr(SSDBServer *serv, const Request &req, Response *resp, int dir, uint64_t version){
	CHECK_NUM_PARAMS(2);

	Transaction trans(serv->ssdb, req[1]);
	int64_t by = 1;
	if(req.size() > 2){
		by = req[2].Int64();
	}
	int64_t new_val;
	int ret = serv->ssdb->incr(req[1], dir * by, &new_val, trans, version);
	if (ret > 0 && serv->binlog) {
		uint64_t encode_by = encode_uint64(by);
		serv->binlog->write(BinlogType::SYNC,
				dir>0 ? BinlogCommand::K_INCR : BinlogCommand::K_DECR,
				req[1], Bytes((char *)&encode_by, sizeof(uint64_t)));
	}

	if(ret == -1){
		resp->reply_status(-1, "ERR value is not an integer or out of range");
	}else{
		resp->reply_int(ret, new_val);
	}
	return 0;
}

int proc_incr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}
	return _incr(serv, req, resp, 1, version);
}

int proc_decr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}
	return _incr(serv, req, resp, -1, version);
}

int proc_getbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int pos = req[2].Int();
	if (pos < 0) {
		resp->push_back("error");
		resp->push_back("ERR bit offset is not an integer or out of range");
		return 0;
	}

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	int ret = 0;
	if(exists) {
		ret = serv->ssdb->getbit(req[1], pos, version);
	}
	resp->reply_bool(ret);
	return 0;
}

int proc_setbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	int offset = req[2].Int();
	if(req[3].size() == 0 || (req[3].data()[0] != '0' && req[3].data()[0] != '1')){
		resp->push_back("client_error");
		resp->push_back("Err bit is not an integer or out of range");
		return 0;
	}
	if(offset < 0 || offset > Link::MAX_PACKET_SIZE * 8){
		std::string msg = "offset is out of range [0, ";
		msg += str(Link::MAX_PACKET_SIZE * 8);
		msg += "]";
		resp->push_back("client_error");
		resp->push_back(msg);
		return 0;
	}
	int on = req[3].Int();

	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}

	Transaction trans(serv->ssdb, req[1]);
	int ret = serv->ssdb->setbit(req[1], offset, on, trans, version);
	if (ret != -1 && serv->binlog) {
		uint64_t uoffset = encode_uint64(offset);
		uint64_t uon = encode_uint64(on);

		std::string soffset((char *)&uoffset, sizeof(uoffset));
		std::string son((char *)&uon, sizeof(uon));
		std::string val = soffset + son;

		serv->binlog->write(BinlogType::SYNC, BinlogCommand::K_SETBIT,
				req[1], Bytes(val.data(), val.size()));
	}

	resp->reply_bool(ret);
	return 0;
}

int proc_countbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
	}
	std::string val;
	int ret = serv->ssdb->get(req[1], &val, version);
	if(ret == -1){
		resp->push_back("error");
		resp->push_back("server inner error");
	}else{
		std::string str;
		int size = -1;
		if(req.size() > 3){
			size = req[3].Int();
			str = substr(val, start, size);
		}else{
			str = substr(val, start, val.size());
		}
		int count = bitcount(str.data(), str.size());
		resp->reply_int(0, count);
	}
	return 0;
}

int proc_bitcount(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
	}
	int end = -1;
	if(req.size() > 3){
		end = req[3].Int();
	}

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_KV(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}
	std::string val;
	int ret = serv->ssdb->get(req[1], &val, version);
	if(ret == -1){
		resp->push_back("error");
		resp->push_back("server inner error");
	}else{
		std::string str = str_slice(val, start, end);
		int count = bitcount(str.data(), str.size());
		resp->reply_int(0, count);
	}
	return 0;
}

/* do not support in cluster */
int proc_substr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("deprecated");
	return 0;
}

int proc_getrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("deprecated");
	return 0;
}

int proc_strlen(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("deprecated");
	return 0;
}
