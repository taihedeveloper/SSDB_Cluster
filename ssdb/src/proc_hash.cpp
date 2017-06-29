/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* hash */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_hexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_bool(0);
		return 0;
	}
	const Bytes &key = req[1];
	const Bytes &field = req[2];
	std::string val;
	int ret = serv->ssdb->hget(key, field, &val, version);
	resp->reply_bool(ret);
	return 0;
}

int proc_multi_hexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_HASH(op);
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
		const Bytes &field = *it;
		int64_t ret = serv->ssdb->hget(key, field, &val, version);
		resp->push_back(field.String());
		if(ret > 0){
			resp->push_back("1");
		}else{
			resp->push_back("0");
		}
	}
	return 0;
}

int proc_multi_hsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("deprecated");
	return 0;
}

int proc_multi_hset(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}
	int num = 0;
	const Bytes &key = req[1];

	Transaction trans(serv->ssdb, key);
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
	for(; it != req.end(); it += 2){
		const Bytes &field = *it;
		const Bytes &val = *(it + 1);
		int ret = serv->ssdb->hset(key, field, val, trans, version);

		if (ret >= 0 && serv->binlog) {
			std::string hkey = encode_hash_key_ex(key, field);
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::H_SET, hkey, val);
		}

		if(ret == -1){
			resp->push_back("error");
			return 0;
		}else{
			num += ret;
		}
	}
	resp->reply_int(0, num);
	return 0;
}

int proc_multi_hdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
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
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}

	int num = 0;
	const Bytes &key = req[1];
	Transaction trans(serv->ssdb, key);
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
	for(; it != req.end(); it += 1){
		const Bytes &field = *it;
		int ret = serv->ssdb->hdel(key, field, trans, version);
		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, BinlogCommand::H_DEL, key, field);
		}
		if(ret == -1){
			resp->push_back("error");
			return 0;
		}else{
			num += ret;
		}
	}
	resp->reply_int(0, num);
	return 0;
}

int proc_multi_hget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_HASH(op);
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
		std::string val;
		int ret = serv->ssdb->hget(key, field, &val, version);
		if(ret == 1){
			resp->push_back(field.String());
			resp->push_back(val);
		}
	}
	return 0;
}

int proc_hsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));
	int16_t migrating_slot = 0;
	GET_SLOT_MIGRATING(serv, resp, migrating_slot);

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}
	int64_t ret = serv->ssdb->hsize(req[1], version);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_hset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
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
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}

	Transaction trans(serv->ssdb, req[1]);
	int ret = serv->ssdb->hset(req[1], req[2], req[3], trans, version);
	if (ret >= 0 && serv->binlog) {
		std::string hkey = encode_hash_key_ex(req[1], req[2]);
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::H_SET, hkey, req[3]);
	}
	resp->reply_bool(ret);
	return 0;
}

int proc_hget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	std::string val;
	int ret = 0;
	if(exists) {
		ret = serv->ssdb->hget(req[1], req[2], &val, version);
	}
	resp->reply_get(ret, &val);
	return 0;
}

int proc_hdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
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
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_bool(0);
		return 0;
	}

	Transaction trans(serv->ssdb, req[1]);
	int ret = serv->ssdb->hdel(req[1], req[2], trans, version);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::H_DEL, req[1], req[2]);
	}

	resp->reply_bool(ret);
	return 0;
}

int proc_hclear(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
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
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:

	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}

	Transaction trans(serv->ssdb, req[1]);
	int64_t count = serv->ssdb->hclear(req[1], trans, version);
	if (count >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::H_CLEAR, req[1]);
	}

	resp->reply_int(0, count);
	return 0;
}

int proc_hgetall(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;
	}
	HIterator *it = serv->ssdb->hscan(req[1], "", "", UINT_MAX, version);
	while(it->next()){
		resp->push_back(it->field);
		resp->push_back(it->val);
	}
	delete it;
	return 0;
}

int proc_hscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;
	}
	uint64_t limit = req[4].Uint64();
	HIterator *it = serv->ssdb->hscan(req[1], req[2], req[3], limit, version);
	while(it->next()){
		resp->push_back(it->field);
		resp->push_back(it->val);
	}
	delete it;
	return 0;
}

int proc_hrscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;
	}
	uint64_t limit = req[4].Uint64();
	HIterator *it = serv->ssdb->hrscan(req[1], req[2], req[3], limit, version);
	while(it->next()){
		resp->push_back(it->field);
		resp->push_back(it->val);
	}
	delete it;
	return 0;
}

int proc_hkeys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;	
	}
	uint64_t limit = req[4].Uint64();
	HIterator *it = serv->ssdb->hscan(req[1], req[2], req[3], limit, version);
	it->return_val(false);

	while(it->next()){
		resp->push_back(it->field);
	}
	delete it;
	return 0;
}

int proc_hvals(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		return 0;	
	}
	uint64_t limit = req[4].Uint64();
	HIterator *it = serv->ssdb->hscan(req[1], req[2], req[3], limit, version);
	while(it->next()){
		resp->push_back(it->val);
	}
	delete it;
	return 0;
}

int proc_hlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("deprecated");
	return 0;
}

int proc_hrlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("error");
	resp->push_back("deprecated");
	return 0;
}

// dir := +1|-1
static int _hincr(SSDBServer *serv, const Request &req, Response *resp, int dir, uint64_t version){
	CHECK_NUM_PARAMS(3);

	Transaction trans(serv->ssdb, req[1]);
	int64_t by = 1;
	if(req.size() > 3){
		by = req[3].Int64();
	}
	int64_t new_val;
	int ret = serv->ssdb->hincr(req[1], req[2], dir * by, &new_val, trans, version);
	if(ret == -1) {
		resp->push_back("error");
		resp->push_back("server inner error");
	}

	if (ret > 0 && serv->binlog) {
		std::string hkey = encode_hash_key_ex(req[1], req[2]);
		uint64_t eby = encode_uint64(by);
		serv->binlog->write(BinlogType::SYNC, dir>0 ? BinlogCommand::H_INCR : BinlogCommand::H_DECR,
				Bytes(hkey.data(), hkey.size()), Bytes((char *)&eby, sizeof(eby)));
	}

	if(ret == 0){
		resp->reply_status(-1, "value is not an integer or out of range");
	}else{
		resp->reply_int(ret, new_val);
	}
	return 0;
}

int proc_hincr(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}
	return _hincr(serv, req, resp, 1, version);
}

int proc_hdecr(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_HASH(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}
	return _hincr(serv, req, resp, -1, version);
}


