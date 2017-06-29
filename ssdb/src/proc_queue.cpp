/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* queue */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_qsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}
	int64_t ret = serv->ssdb->qsize(req[1], version);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_qfront(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	std::string item;
	int ret = 0;
	if(exists) {
		ret = serv->ssdb->qfront(req[1], &item, version);
	}
	resp->reply_get(ret, &item);
	return 0;
}

int proc_qback(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	std::string item;
	int ret = 0;
	if(exists) {
		ret = serv->ssdb->qback(req[1], &item, version);
	}
	resp->reply_get(ret, &item);
	return 0;
}

static int QFRONT = 2;
static int QBACK  = 3;

static inline
int proc_qpush_func(NetworkServer *net, Link *link, const Request &req, Response *resp, int front_or_back){
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
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		NEW_VERSION(req[1], op, version);
	}
	int64_t size = 0;
	std::vector<Bytes>::const_iterator it;
	it = req.begin() + 2;

	Transaction trans(serv->ssdb, req[1]);
	for(; it != req.end(); it += 1){
		const Bytes &item = *it;
		if(front_or_back == QFRONT){
			size = serv->ssdb->qpush_front(req[1], item, trans, version);
		}else{
			size = serv->ssdb->qpush_back(req[1], item, trans, version);
		}

		// write binlog
		if (size >= 0 && serv->binlog) {
			serv->binlog->write(BinlogType::SYNC, front_or_back==QFRONT ?
					BinlogCommand::Q_PUSH_FRONT : BinlogCommand::Q_PUSH_BACK, req[1], item);
		}
		if(size == -1){
			resp->push_back("error");
			return 0;
		}
	}
	resp->reply_int(0, size);
	return 0;
}

int proc_qpush_front(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_qpush_func(net, link, req, resp, QFRONT);
}

int proc_qpush_back(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_qpush_func(net, link, req, resp, QBACK);
}

int proc_qpush(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_qpush_func(net, link, req, resp, QBACK);
}


static inline
int proc_qpop_func(NetworkServer *net, Link *link, const Request &req, Response *resp, int front_or_back){
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
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	uint64_t size = 1;
	if(req.size() > 2){
		size = req[2].Uint64();
	}
	int ret = 0;
	std::string item;
	if(!exists || size <= 0) {
		resp->reply_get(ret, &item);
		return 0;
	}

	Transaction trans(serv->ssdb, req[1]);
	if(size == 1){
		if(front_or_back == QFRONT){
			ret = serv->ssdb->qpop_front(req[1], &item, trans, version);
		}else{
			ret = serv->ssdb->qpop_back(req[1], &item, trans, version);
		}
		resp->reply_get(ret, &item);
	}else{
		resp->push_back("ok");
		while(size-- > 0){
			if(front_or_back == QFRONT){
				ret = serv->ssdb->qpop_front(req[1], &item, trans, version);
			}else{
				ret = serv->ssdb->qpop_back(req[1], &item, trans, version);
			}
			if(ret <= 0){
				break;
			}else{
				resp->push_back(item);
			}
		}
	}

	// write binlog
	if (ret > 0 && serv->binlog) {
		uint64_t esize = encode_uint64(size);
		serv->binlog->write(BinlogType::SYNC, front_or_back==QFRONT ?
				BinlogCommand::Q_POP_FRONT : BinlogCommand::Q_POP_BACK, req[1],
				Bytes((char *)&esize, sizeof(uint64_t)));
	}

	return 0;
}

int proc_qpop_front(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_qpop_func(net, link, req, resp, QFRONT);
}

int proc_qpop_back(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_qpop_func(net, link, req, resp, QBACK);
}

int proc_qpop(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_qpop_func(net, link, req, resp, QFRONT);
}

int proc_qltrim(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->push_back("ok");
		return 0;
	}
	/* possible inconsistent */
	int64_t start = req[2].Int64();
	int64_t stop = req[3].Int64();
	std::string item;
	int64_t front_num = 0;
	int64_t back_num = 0;
	Transaction trans(serv->ssdb, req[1]);

	int64_t len = serv->ssdb->qsize(req[1], version);
	if (len < 0) {
		goto fail;
	}

	start = start < 0 ? start + len : start;
	start = start < 0 ? 0 : start;
	stop = stop < 0 ? stop + len : stop;
	stop = stop < 0 ? 0 : stop;

	for (int64_t i = 0; i < start; i++) {
		int ret = serv->ssdb->qpop_front(req[1], &item, trans, version);
		if (ret < 0) {
			goto fail;
		}
		if (ret == 0) {
			goto finish;
		}
		front_num++;
	}

	for (int64_t i = len - 1; i > stop; i--) {
		int ret = serv->ssdb->qpop_back(req[1], &item, trans, version);
		if (ret < 0) {
			goto fail;
		}
		if (ret == 0) {
			goto finish;
		}
		back_num++;
	}

finish:
	if (front_num > 0) {
		uint64_t esize = encode_uint64(front_num);
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_POP_FRONT, req[1], Bytes((char *)&esize, sizeof(uint64_t)));
	}
	if (back_num > 0) {
		uint64_t esize = encode_uint64(back_num);
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_POP_BACK, req[1], Bytes((char *)&esize, sizeof(uint64_t)));
	}
	resp->push_back("ok");
	return 0;

fail:
	resp->push_back("error");
	return 0;
}

static inline
int proc_qtrim_func(NetworkServer *net, Link *link, const Request &req, Response *resp, int front_or_back){
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
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}
	uint64_t size = 1;
	if(req.size() > 2){
		size = req[2].Uint64();
	}

	Transaction trans(serv->ssdb, req[1]);

	int count = 0;
	for(; count<size; count++){
		int ret;
		std::string item;
		if(front_or_back == QFRONT){
			ret = serv->ssdb->qpop_front(req[1], &item, trans, version);
		}else{
			ret = serv->ssdb->qpop_back(req[1], &item, trans, version);
		}
		if(ret <= 0){
			break;
		}
	}

	if (size > 0 && serv->binlog) {
		uint64_t esize = encode_uint64(size);
		serv->binlog->write(BinlogType::SYNC, front_or_back==QFRONT ?
				BinlogCommand::Q_POP_FRONT : BinlogCommand::Q_POP_BACK,
				req[1], Bytes((char *)&esize, sizeof(uint64_t)));
	}

	resp->reply_int(0, count);

	return 0;
}

int proc_qtrim_front(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_qtrim_func(net, link, req, resp, QFRONT);
}

int proc_qtrim_back(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_qtrim_func(net, link, req, resp, QBACK);
}

int proc_qlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("deprecated");
	return 0;
}

int proc_qrlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("deprecated");
	return 0;
}

int proc_qfix(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->push_back("ok");
		return 0;
	}
	Transaction trans(serv->ssdb, req[1]);
	int ret = serv->ssdb->qfix(req[1], trans);
	if (ret >= 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_FIX, req[1]);
	}
	if(ret == -1){
		resp->push_back("error");
	}else{
		resp->push_back("ok");
	}

	return 0;
}

int proc_qclear(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->reply_int(0, 0);
		return 0;
	}

	Transaction trans(serv->ssdb, req[1]);
	int64_t count = serv->ssdb->qclear(req[1], trans, version);
	if (count == -1) {
		resp->push_back("error");
		return 0;
	}
	if (count > 0 && serv->binlog) {
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_CLEAR, req[1]);
	}
	resp->reply_int(0, count);
	return 0;
}

int proc_qslice(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	int64_t begin = req[2].Int64();
	int64_t end = req[3].Int64();
	std::vector<std::string> list;
	int ret = 0;
	if(exists) {
		ret = serv->ssdb->qslice(req[1], begin, end, version, &list);
	}
	resp->reply_list(ret, list);
	return 0;
}

int proc_qrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	int64_t begin = req[2].Int64();
	int64_t limit = req[3].Uint64();
	int64_t end;
	if(limit >= 0){
		end = begin + limit - 1;
	}else{
		end = -1;
	}
	std::vector<std::string> list;
	int ret = 0;
	if(exists) {
		ret = serv->ssdb->qslice(req[1], begin, end, version, &list);
	}
	resp->reply_list(ret, list);
	return 0;
}

int proc_qget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	CHECK_KEY(req[1]);
	int16_t slot = KEY_HASH_SLOT(req[1]);
	ReadLockGuard<RWLock> slot_guard(serv->ssdb_cluster->get_state_lock(slot));

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	int64_t index = req[2].Int64();
	std::string item;
	int ret = 0;
	if(exists) {
		ret = serv->ssdb->qget(req[1], index, &item, version);
	}
	resp->reply_get(ret, &item);
	return 0;
}

int proc_qset(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
	CHECK_DATA_TYPE_QUEUE(op);
	CHECK_ASK(req[1]);
	CHECK_ASKING(serv, link, resp, slot);

action:
	if(!exists) {
		resp->push_back("error");
		resp->push_back("ERR no such key");
		return 0;
	}
	const Bytes &key = req[1];
	int64_t index = req[2].Int64();
	const Bytes &item = req[3];

	Transaction trans(serv->ssdb, key);
	int ret = serv->ssdb->qset(key, index, item, trans, version);
	if (ret > 0 && serv->binlog) {
		uint64_t eindex = encode_uint64(index);
		std::string item_key = encode_qitem_key_ex(key, eindex);
		serv->binlog->write(BinlogType::SYNC, BinlogCommand::Q_SET,
				Bytes(item_key.data(), item_key.size()), item);
	}

	if(ret == -1 || ret == 0){
		resp->push_back("error");
		resp->push_back("index out of range");
	}else{
		resp->push_back("ok");
	}
	return 0;
}
