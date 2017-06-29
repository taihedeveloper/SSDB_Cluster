/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_SERVER_H_
#define SSDB_SERVER_H_

#include "include.h"
#include <map>
#include <vector>
#include <string>
#include "ssdb/ssdb_impl.h"
#include "ssdb/ttl.h"
#include "backend_dump.h"
#include "backend_sync2.h"
#include "slave.h"
#include "net/server.h"
#include "cluster.h"
#include "ssdb/binlog2.h"

class KeyLock;
class SSDBServer
{
private:
	void reg_procs(NetworkServer *net);

	std::string kv_range_s;
	std::string kv_range_e;

	SSDB *meta;

public:
	SSDBImpl *ssdb;
	BackendDump *backend_dump;
	BackendSync *backend_sync;
	ExpirationHandler *expiration;
	std::vector<Slave *> slaves;
	Cluster *cluster; /* deprecated, use SSDBCluster instead */
	SSDBCluster *ssdb_cluster;
	NetworkServer *net;
	Config *config;
	RWLock config_lock;

	std::string conf_file;
	std::string local_ip;
	int local_port;
	std::string local_tag;
	std::string auth;

	// global read lock
	volatile bool global_read_lock;
	Slave *slave;

	// binlog
	SSDB_BinLog *binlog;

	SSDBServer(SSDB *ssdb, SSDB *meta, Config *conf, NetworkServer *net, SSDB_BinLog *binlog=NULL);
	~SSDBServer();

	int set_kv_range(const std::string &s, const std::string &e);
	int get_kv_range(std::string *s, std::string *e);
	bool in_kv_range(const std::string &key);
	bool in_kv_range(const Bytes &key);

	void init_slave(const Config &conf);
};

#define CHECK_KV_KEY_RANGE(n) do{ \
		if(!link->ignore_key_range && req.size() > n){ \
			if(!serv->in_kv_range(req[n])){ \
				resp->push_back("out_of_range"); \
				return 0; \
			} \
		} \
	}while(0)

#define CHECK_NUM_PARAMS(n) do{ \
		if(req.size() < n){ \
			resp->clear(); \
			resp->push_back("client_error"); \
			resp->push_back("wrong number of arguments"); \
			return 0; \
		} \
	}while(0)

#define CHECK_ASKING(serv, link, resp, slot) \
do { \
	int flag = 0;\
	int ret = serv->ssdb_cluster->test_slot_importing(slot, &flag); \
	if(ret != 0) { \
		log_warn("test slot importing failed");\
		return 0;\
	}\
	if(flag) { \
		if(link->asking) { \
			link->asking = false; \
			break; \
		} else { \
			resp->clear(); \
			resp->push_back("error"); \
			resp->push_back("server migrating"); \
		} \
		return 0; \
	} \
} while(0)

#define GET_SLOT_MIGRATING(serv, resp, slot) \
do { \
	int ret = serv->ssdb_cluster->get_slot_migrating(&slot); \
	if(ret != 0) { \
		log_warn("get slot migrating failed"); \
		resp->clear(); \
		resp->push_back("error"); \
		resp->push_back("server inner error"); \
		return 0; \
	} \
} while(0)

#define CHECK_READ_ONLY \
do { \
	if (serv->global_read_lock) { \
		resp->clear(); \
		resp->push_back("error"); \
		resp->push_back("ssdb is read only"); \
		return 0; \
	} \
} while(0)

#define CHECK_META(key, op, version, exists) \
do { \
	exists = serv->ssdb->get_version(key, &op, &version); \
	if(exists == -1) { \
		resp->clear(); \
		resp->push_back("error"); \
		resp->push_back("server inner error"); \
		return 0; \
	} \
} while(0)

#define CHECK_DATA_TYPE(t1, t2) \
do { \
	if(exists) { \
		if(t1 != t2) { \
			resp->clear(); \
			resp->push_back("error"); \
			resp->push_back("WRONGTYPE Operation against a key holding the wrong kind of value"); \
			log_error("WRONGTYPE Operation against a key holding the wrong kind of value"); \
			return 0; \
		} \
	} else { \
		t1 = t2; \
	} \
} while(0)

#define CHECK_DATA_TYPE_KV(t)    CHECK_DATA_TYPE(t, DataType::KV)
#define CHECK_DATA_TYPE_HASH(t)  CHECK_DATA_TYPE(t, DataType::HASH)
#define CHECK_DATA_TYPE_SET(t)   CHECK_DATA_TYPE(t, DataType::SET)
#define CHECK_DATA_TYPE_ZSET(t)  CHECK_DATA_TYPE(t, DataType::ZSET)
#define CHECK_DATA_TYPE_QUEUE(t) CHECK_DATA_TYPE(t, DataType::QUEUE)

#define CHECK_ASK(user_key) \
do { \
	int16_t migrating_slot = 0; \
	GET_SLOT_MIGRATING(serv, resp, migrating_slot); \
	if(migrating_slot == slot) { \
		KeyLock &key_lock = serv->ssdb_cluster->get_key_lock(user_key.String()); \
		ReadLockGuard<KeyLock> key_guard(key_lock); \
		if(key_lock.test_key(user_key.String())) { \
			resp->clear(); \
			resp->push_back("error"); \
			resp->push_back("tryagain"); \
		} else if (!exists) { \
			resp->clear(); \
			resp->push_back("error"); \
			resp->push_back("ask"); \
		} else { \
			goto action; \
		} \
		return 0; \
	} \
} while(0)

#define NEW_VERSION(user_key, op, version) \
do { \
	int ret = serv->ssdb->new_version(user_key, op, &version); \
	if(ret == -1) { \
		resp->clear(); \
		resp->push_back("error"); \
		resp->push_back("server inner error"); \
		return 0;\
	}\
} while(0)

#define CHECK_SLOT_MOVED(slot) \
do { \
	int flag = 0; \
	serv->ssdb_cluster->test_slot(slot, &flag); \
	if(!flag) { \
		resp->clear(); \
		resp->push_back("error"); \
		resp->push_back("moved"); \
		log_error("slot %d moved", slot); \
		return 0; \
	} \
} while(0)

#define CHECK_KEY(key) \
do { \
	if((key).data()[0] == '\xff') { \
		resp->clear(); \
		resp->push_back("error"); \
		resp->push_back("invaildate key"); \
		return 0; \
	} \
} while(0)

#endif
