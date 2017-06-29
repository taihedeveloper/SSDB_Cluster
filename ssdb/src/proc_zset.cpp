/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* zset */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

/* key: req[1] field: req[2] */
int proc_zexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_bool(0);
		return 0;
	}

	std::string val;
	int ret = serv->ssdb->zget(req[1], req[2], &val, version);
	resp->reply_bool(ret);
	return 0;
}

int proc_multi_zexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;
	}
	const Bytes &key = req[1];
	std::string val;
	for(Request::const_iterator it=req.begin()+2; it!=req.end(); it++){
		int64_t ret = serv->ssdb->zget(key, *it, &val, version);
		resp->push_back(it->String());
		if(ret > 0){
			resp->push_back("1");
		}else{
			resp->push_back("0");
		}
	}
	return 0;
}


int proc_multi_zsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("deprecated");
	return 0;
}

int proc_multi_zset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;
	if(req.size() < 4 || req.size() % 2 != 0){
		resp->push_back("client_error");
		return 0;
	}

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	CHECK_SLOT_MOVED(slot);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}

	int num = 0;
	const Bytes &key = req[1];

	// TODO: to gurantee mul_set atomic
	Transaction trans(serv->ssdb, key);
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
	for(; it != req.end(); it += 2){
		const Bytes &field = *it;
		const Bytes &val = *(it + 1);

		// gurantee:
		// 1) atomic of zset;
		// 2) order of the update in db and order of binlog is the same
		// for the same key.
		int ret = serv->ssdb->zset(key, field, val, trans, version);

		if (ret >= 0 && serv->binlog) {
			std::string zkey = encode_zset_key_ex(key, field);
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_SET, zkey, val);
		}

		if(ret == -1){
			resp->push_back("error");
			resp->push_back("server inner error");
			return 0;
		}else{
			num += ret;
		}
	}

	resp->reply_int(0, num);
	return 0;
}

int proc_multi_zdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}

	int num = 0;
	const Bytes &key = req[1];
	std::vector<Bytes>::const_iterator it = req.begin() + 2;

	// TODO: gurantee mul_del atomic
	Transaction trans(serv->ssdb, key);
	for(; it != req.end(); it += 1){
		const Bytes &field = *it;

		int ret = serv->ssdb->zdel(key, field, trans, version);

		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_DEL,	key, field);
		}

		if(ret == -1){
			resp->push_back("error");
			resp->push_back("server inner error");
			return 0;
		}else{
			num += ret;
		}
	}
	resp->reply_int(0, num);
	return 0;
}

int proc_multi_zget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;
	}

	Request::const_iterator it=req.begin() + 1;
	const Bytes key = *it;
	it ++;
	for(; it!=req.end(); it+=1){
		const Bytes &field = *it;
		std::string score;
		int ret = serv->ssdb->zget(key, field, &score, version);
		if(ret == 1){
			resp->push_back(field.String());
			resp->push_back(score);
		}
	}
	return 0;
}

int proc_zset(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}
	log_info("zset version %"PRIu64, version);

	Transaction trans(serv->ssdb, req[1]);
	int ret = serv->ssdb->zset(req[1], req[2], req[3], trans, version);
	if (ret >= 0 && serv->binlog) {
		std::string key = encode_zset_key_ex(req[1], req[2]);
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_SET,
				Bytes(key.data(), key.size()), req[3]);
	}
	resp->reply_int(ret, ret);
	return 0;
}


int proc_zsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
	}
	int ret = serv->ssdb->zsize(req[1], version);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_zget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));


	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	std::string score;
	int ret = 0;
	if(exists) {
		ret = serv->ssdb->zget(req[1], req[2], &score, version);
	}
	resp->reply_get(ret, &score);
	return 0;
}

int proc_zdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_bool(0);
	}

	std::string key(req[1].data(), req[1].size());
	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->zdel(req[1], req[2], trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_DEL,
				req[1], req[2]);
	}
	resp->reply_bool(ret);
	return 0;
}

int proc_zrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	int64_t ret = -1;
	if(exists) {
		ret = serv->ssdb->zrank(req[1], req[2], version);
	}
	if(ret == -1){
		resp->add("not_found");
	}else{
		resp->reply_int(ret, ret);
	}
	return 0;
}

int proc_zrrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->add("not_found");
		return 0;
	}
	int64_t ret = serv->ssdb->zrrank(req[1], req[2], version);
	if(ret == -1){
		resp->add("not_found");
	}else{
		resp->reply_int(ret, ret);
	}
	return 0;
}

int proc_zrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;
	}
	int64_t start = req[2].Int64();
	int64_t stop = req[3].Int64();
	ZIterator *it = serv->ssdb->zrange(req[1], start, stop, version);
	if(it == NULL) {
		return 0;
	}

	while(it->next()){
		resp->push_back(it->field);
		resp->push_back(it->score);
	}
	delete it;
	return 0;
}

int proc_zrrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;
	}
	int64_t start = req[2].Int64();
	int64_t stop = req[3].Int64();
	ZIterator *it = serv->ssdb->zrrange(req[1], start, stop, version);
	if(it == NULL) {
		return 0;
	}

	while(it->next()){
		resp->push_back(it->field);
		resp->push_back(it->score);
	}
	delete it;
	return 0;
}

int proc_zclear(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}

	Transaction trans(serv->ssdb, req[1]);
	int64_t count = serv->ssdb->zclear(req[1], trans, version);
	if (count == -1) {
		resp->push_back("error");
		return 0;
	}

	if (serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_CLEAR, req[1]);
	}
	resp->reply_int(0, count);
	return 0;
}

int proc_zscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(6);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;	
	}
	uint64_t limit = req[5].Uint64();
	uint64_t offset = 0;
	if(req.size() > 6){
		offset = limit;
		limit = offset + req[6].Uint64();
	}
	ZIterator *it = serv->ssdb->zscan(req[1], req[2], req[3], req[4], limit, version);
	if(offset > 0){
		it->skip(offset);
	}
	while(it->next()){
		resp->push_back(it->field);
		resp->push_back(it->score);
	}
	delete it;
	return 0;
}

int proc_zrscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(6);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;	
	}
	uint64_t limit = req[5].Uint64();
	uint64_t offset = 0;
	if(req.size() > 6){
		offset = limit;
		limit = offset + req[6].Uint64();
	}
	ZIterator *it = serv->ssdb->zrscan(req[1], req[2], req[3], req[4], limit, version);
	if(offset > 0){
		it->skip(offset);
	}
	while(it->next()){
		resp->push_back(it->field);
		resp->push_back(it->score);
	}
	delete it;
	return 0;
}

int proc_zkeys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(6);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;
	}
	uint64_t limit = req[5].Uint64();
	ZIterator *it = serv->ssdb->zscan(req[1], req[2], req[3], req[4], limit, version);
	while(it->next()){
		resp->push_back(it->field);
	}
	delete it;
	return 0;
}

int proc_zlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	uint64_t limit = req[3].Uint64();
	std::vector<std::string> list;
	if(!exists) {
		resp->reply_list(0, list);
		return 0;	
	}
	int ret = serv->ssdb->zlist(req[1], req[2], limit, &list);
	resp->reply_list(ret, list);
	return 0;
}

int proc_zrlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	uint64_t limit = req[3].Uint64();
	std::vector<std::string> list;
	if(!exists) {
		resp->reply_list(0, list);
		return 0;
	}
	int ret = serv->ssdb->zrlist(req[1], req[2], limit, &list);
	resp->reply_list(ret, list);
	return 0;
}

// dir := +1|-1
static int _zincr(SSDBServer *serv, const Request &req, Response *resp, int dir, uint64_t version){
	CHECK_NUM_PARAMS(3);

	int64_t by = 1;
	if(req.size() > 3){
		by = req[3].Int64();
	}

	Transaction trans(serv->ssdb, req[1]);

	int64_t new_val;
	int ret = serv->ssdb->zincr(req[1], req[2], dir * by, &new_val, trans, version);

	if (ret >= 0 && serv->binlog) {
		std::string key = encode_zset_key_ex(req[1], req[2]);

		uint64_t eby = encode_uint64(by);
		Bytes val((char *)&eby, sizeof(eby));

		serv->binlog->write(BinlogType::SYNC,
				dir>0 ? BinlogCommand::Z_INCR : BinlogCommand::Z_DECR, key, val);
	}

	resp->reply_int(ret, new_val);
	return 0;
}

int proc_zincr(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}
	return _zincr(serv, req, resp, 1, version);
}

int proc_zdecr(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op,version);
	}
	return _zincr(serv, req, resp, -1, version);
}

int proc_zcount(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}
	int64_t count = 0;
	ZIterator *it = serv->ssdb->zscan(req[1], "", req[2], req[3], UINT_MAX, version);
	while(it->next()){
		count ++;
	}
	delete it;

	resp->reply_int(0, count);
	return 0;
}

int proc_zsum(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	int64_t sum = 0;
	if(exists) {
		ZIterator *it = serv->ssdb->zscan(req[1], "", req[2], req[3], -1, version);
		while(it->next()){
			sum += str_to_int64(it->score);
		}
		delete it;
	}

	resp->reply_int(0, sum);
	return 0;
}

int proc_zavg(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		resp->add(0);
		return 0;
	}
	int64_t sum = 0;
	int64_t count = 0;
	ZIterator *it = serv->ssdb->zscan(req[1], "", req[2], req[3], -1, version);
	while(it->next()){
		sum += str_to_int64(it->score);
		count ++;
	}
	delete it;
	double avg = (double)sum/count;
	resp->add(avg);
	return 0;
}

int proc_zremrangebyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}

	Transaction trans(serv->ssdb, req[1]);
	ZIterator *it = serv->ssdb->zscan(req[1], "", req[2], req[3], UINT_MAX, version);
	int64_t count = 0;
	while(it->next()){
		count ++;

		int ret = serv->ssdb->zdel(req[1], it->field, trans, version);

		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_DEL,
					req[1], it->field);
		}

		if(ret == -1){
			delete it;
			resp->push_back("error");
			return 0;
		}
	}
	delete it;

	resp->reply_int(0, count);
	return 0;
}

int proc_zremrangebyrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}

    int64_t start = req[2].Int64();
    int64_t stop = req[3].Int64();
	int64_t count = 0;

	Transaction trans(serv->ssdb, req[1]);
    ZIterator *it = serv->ssdb->zrange(req[1], start, stop, version);
    if(it == NULL) {
		resp->reply_int(0, count);
		return 0;
    }

	while(it->next()){
		count ++;
		int ret = serv->ssdb->zdel(req[1], it->field, trans, version);
		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_DEL, req[1], it->field);
		}
		if(ret == -1){
			resp->push_back("error");
			delete it;
			return 0;
		}
	}
	delete it;

	resp->reply_int(0, count);
	return 0;
}

static inline
void zpop(ZIterator *it, SSDBServer *serv, const Bytes &key, Response *resp, Transaction &trans){
	resp->push_back("ok");
	while(it->next()){
		int ret = serv->ssdb->zdel(key, it->field, trans, it->version);
		if(ret == 1){
			resp->add(it->field);
			resp->add(it->score);
		}
	}
}

int proc_zpop_front(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->push_back("ok");
		return 0;
	}

	const Bytes &key = req[1];
	uint64_t limit = req[2].Uint64();

	Transaction trans(serv->ssdb, key);
	ZIterator *it = serv->ssdb->zscan(key, "", "", "", limit, version);
	zpop(it, serv, key, resp, trans);
	delete it;
	if (serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_POP_FRONT, key, req[2]);
	}
	return 0;
}

int proc_zpop_back(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_ZSET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}
	const Bytes &key = req[1];
	uint64_t limit = req[2].Uint64();

	Transaction trans(serv->ssdb, key);
	ZIterator *it = serv->ssdb->zrscan(key, "", "", "", limit, version);
	zpop(it, serv, key, resp, trans);
	delete it;

	if (serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Z_POP_BACK, key, req[2]);
	}
	return 0;
}

