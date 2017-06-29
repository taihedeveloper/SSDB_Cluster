/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* sset */
#include <stdint.h>
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_sismember(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_SET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	resp->push_back("ok");
	if(!exists) {
		resp->push_back("0");
		return 0;
	}
	int ret = serv->ssdb->sget(req[1], req[2], version);
	if(ret > 0) {
		resp->push_back("1");
	} else {
		resp->push_back("0");
	}
	return 0;
}

int proc_ssize(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_SET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}
	int64_t ret = serv->ssdb->ssize(req[1], version);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_multi_sset(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_SET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	int num = 0;
	const Bytes &key = req[1];
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
	if(!exists) {
		NEW_VERSION(key, op, version);
	}

	Transaction trans(serv->ssdb, key);
	for (; it != req.end(); ++it) {
		int ret = serv->ssdb->sset(key, *it, trans, version);
		if (ret >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC,
				BinlogCommand::S_SET, key, *it);
		} else if (ret < 0) {
			resp->push_back("error");
			return 0;
		}
        num += ret;
	}
	resp->reply_int(0, num);
	return 0;
}

int proc_multi_sdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_SET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	int num = 0;
	const Bytes &key = req[1];
	std::vector<Bytes>::const_iterator it = req.begin() + 2;

	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}

	Transaction trans(serv->ssdb, key);
	for (; it != req.end(); ++it) {
		int ret = serv->ssdb->sdel(key, *it, trans, version);
		if (ret >=0 && serv->binlog) {
			num += ret;
			serv->binlog->write(BinlogType::SYNC,
				BinlogCommand::S_DEL, key, *it);
		} else {
			resp->push_back("error");
			return 0;
		}
	}

	resp->reply_int(0, num);
	return 0;
}

int proc_sscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_SET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->push_back("ok");
		return 0;
	}

	uint64_t limit = req[3].Uint64();
	uint64_t offset = 0;
	if(req.size() > 4){
		offset = limit;
		limit = offset + req[4].Uint64();
	}
	SIterator *it = serv->ssdb->sscan(req[1], req[2], limit, version);
	if(offset > 0){
		it->skip(offset);
	}
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->elem);
	}
	delete it;
	return 0;
}

int proc_srscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_SET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->push_back("ok");
		return 0;
	}

	uint64_t limit = req[3].Uint64();
	uint64_t offset = 0;
	if(req.size() > 4){
		offset = limit;
		limit = offset + req[4].Uint64();
	}
	SIterator *it = serv->ssdb->srscan(req[1], req[2], limit, version);
	if(offset > 0){
		it->skip(offset);
	}
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->elem);
	}
	delete it;
	return 0;
}

int proc_smembers(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_SET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->push_back("ok");
		return 0;
	}

	uint64_t limit = UINT64_MAX;
	SIterator *it = serv->ssdb->sscan(req[1], "", limit, version);
	resp->push_back("ok");
	while(it->next()) {
		resp->push_back(it->elem);
	}
	delete it;
	return 0;
}

int proc_sclear(NetworkServer *net, Link *link, const Request &req, Response *resp) {
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
	CHECK_DATA_TYPE_SET(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}

	Transaction trans(serv->ssdb, req[1]);
	int64_t count = serv->ssdb->sclear(req[1], trans, version);
	if (count == -1) {
		resp->push_back("error");
		return 0;
	}

	if (count >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::S_CLEAR, req[1]);
	}

	resp->reply_int(0, count);
	return 0;
}
