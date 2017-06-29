/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "version.h"
#include "util/log.h"
#include "util/strings.h"
#include "util/thread.h"
#include "util/spin_lock.h"
#include "util/net.h"
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

DEF_PROC(get);
DEF_PROC(set);
DEF_PROC(setx);
DEF_PROC(setnx);
DEF_PROC(getset);
DEF_PROC(getbit);
DEF_PROC(setbit);
DEF_PROC(countbit);
DEF_PROC(substr);
DEF_PROC(getrange);
DEF_PROC(strlen);
DEF_PROC(bitcount);
DEF_PROC(del);
DEF_PROC(incr);
DEF_PROC(decr);
DEF_PROC(scan);
DEF_PROC(rscan);
DEF_PROC(keys);
DEF_PROC(rkeys);
DEF_PROC(exists);
DEF_PROC(multi_exists);
DEF_PROC(multi_get);
DEF_PROC(multi_set);
DEF_PROC(multi_del);
DEF_PROC(ttl);
DEF_PROC(expire);
DEF_PROC(expire_at);
DEF_PROC(pexpire);
DEF_PROC(pexpire_at);

DEF_PROC(hsize);
DEF_PROC(hget);
DEF_PROC(hset);
DEF_PROC(hdel);
DEF_PROC(hincr);
DEF_PROC(hdecr);
DEF_PROC(hclear);
DEF_PROC(hgetall);
DEF_PROC(hscan);
DEF_PROC(hrscan);
DEF_PROC(hkeys);
DEF_PROC(hvals);
DEF_PROC(hlist);
DEF_PROC(hrlist);
DEF_PROC(hexists);
DEF_PROC(multi_hexists);
DEF_PROC(multi_hsize);
DEF_PROC(multi_hget);
DEF_PROC(multi_hset);
DEF_PROC(multi_hdel);

DEF_PROC(multi_sset);
DEF_PROC(ssize);
DEF_PROC(multi_sdel);
DEF_PROC(sismember);
DEF_PROC(smembers);
//DEF_PROC(sdiff);
//DEF_PROC(sdiffstore);
//DEF_PROC(inter);
//DEF_PROC(interstore);
//DEF_PROC(srem);
//DEF_PROC(smove);
//DEF_PROC(spop);
//DEF_PROC(srandmember);
//DEF_PROC(sscan);
//DEF_PROC(sunion);
//DEF_PROC(sunionstore);

DEF_PROC(zrank);
DEF_PROC(zrrank);
DEF_PROC(zrange);
DEF_PROC(zrrange);
DEF_PROC(zsize);
DEF_PROC(zget);
DEF_PROC(zset);
DEF_PROC(zdel);
DEF_PROC(zincr);
DEF_PROC(zdecr);
DEF_PROC(zclear);
DEF_PROC(zscan);
DEF_PROC(zrscan);
DEF_PROC(zkeys);
DEF_PROC(zlist);
DEF_PROC(zrlist);
DEF_PROC(zcount);
DEF_PROC(zsum);
DEF_PROC(zavg);
DEF_PROC(zexists);
DEF_PROC(zremrangebyrank);
DEF_PROC(zremrangebyscore);
DEF_PROC(multi_zexists);
DEF_PROC(multi_zsize);
DEF_PROC(multi_zget);
DEF_PROC(multi_zset);
DEF_PROC(multi_zdel);
DEF_PROC(zpop_front);
DEF_PROC(zpop_back);

DEF_PROC(qsize);
DEF_PROC(qfront);
DEF_PROC(qback);
DEF_PROC(qpush);
DEF_PROC(qpush_front);
DEF_PROC(qpush_back);
DEF_PROC(qpop);
DEF_PROC(qpop_front);
DEF_PROC(qpop_back);
DEF_PROC(qtrim_front);
DEF_PROC(qtrim_back);
DEF_PROC(qfix);
DEF_PROC(qclear);
DEF_PROC(qlist);
DEF_PROC(qrlist);
DEF_PROC(qslice);
DEF_PROC(qrange);
DEF_PROC(qget);
DEF_PROC(qset);
DEF_PROC(qltrim);

DEF_PROC(dump);
DEF_PROC(dump_slot);
DEF_PROC(sync140);
DEF_PROC(info);
DEF_PROC(version);
DEF_PROC(dbsize);
DEF_PROC(compact);
DEF_PROC(flushdb);

DEF_PROC(purge_logs_to);
DEF_PROC(purge_logs_before);

DEF_PROC(get_key_range);
DEF_PROC(ignore_key_range);
DEF_PROC(get_kv_range);
DEF_PROC(set_kv_range);

DEF_PROC(cluster_add_kv_node);
DEF_PROC(cluster_del_kv_node);
DEF_PROC(cluster_kv_node_list);
DEF_PROC(cluster_set_kv_range);
DEF_PROC(cluster_set_kv_status);
DEF_PROC(cluster_migrate_kv_data);

/* migration */
DEF_PROC(migrate_slot);
DEF_PROC(migrate);
DEF_PROC(flag_migrating);
DEF_PROC(flag_importing);
DEF_PROC(flag_normal);
DEF_PROC(migrate_result);
DEF_PROC(slot_premigrating);
DEF_PROC(slot_postmigrating);
DEF_PROC(slot_preimporting);
DEF_PROC(slot_postimporting);
DEF_PROC(asking);
DEF_PROC(set_slot);
DEF_PROC(unset_slot);
DEF_PROC(type);

/* testing */
DEF_PROC(lock_key);
DEF_PROC(unlock_key);
DEF_PROC(key_slot);

/* global read lock */
DEF_PROC(lock_db_with_read_lock);
DEF_PROC(unlock_db);

/* replicate */
DEF_PROC(show_slave_status);
DEF_PROC(change_master_to);
DEF_PROC(start_slave);
DEF_PROC(stop_slave);

/* configuration */
DEF_PROC(config);

/* others */
DEF_PROC(client_pause);

#define REG_PROC(c, f)     net->proc_map.set_proc(#c, f, proc_##c)

void SSDBServer::reg_procs(NetworkServer *net){
	REG_PROC(get, "rt");
	REG_PROC(set, "wt");
	REG_PROC(del, "wt");
	REG_PROC(setx, "wt");
	REG_PROC(setnx, "wt");
	REG_PROC(getset, "wt");
	REG_PROC(getbit, "rt");
	REG_PROC(setbit, "wt");
	REG_PROC(countbit, "rt");
	REG_PROC(substr, "rt");
	REG_PROC(getrange, "rt");
	REG_PROC(strlen, "rt");
	REG_PROC(bitcount, "rt");
	REG_PROC(incr, "wt");
	REG_PROC(decr, "wt");
	REG_PROC(scan, "rt");
	REG_PROC(rscan, "rt");
	REG_PROC(keys, "rt");
	REG_PROC(rkeys, "rt");
	REG_PROC(exists, "rt");
	REG_PROC(multi_exists, "rt");
	REG_PROC(multi_get, "rt");
	REG_PROC(multi_set, "wt");
	REG_PROC(multi_del, "wt");
	REG_PROC(ttl, "rt");
	REG_PROC(expire, "wt");
	REG_PROC(expire_at, "wt");
	REG_PROC(pexpire, "wt");
	REG_PROC(pexpire_at, "wt");

	REG_PROC(hsize, "rt");
	REG_PROC(hget, "rt");
	REG_PROC(hset, "wt");
	REG_PROC(hdel, "wt");
	REG_PROC(hincr, "wt");
	REG_PROC(hdecr, "wt");
	REG_PROC(hclear, "wt");
	REG_PROC(hgetall, "rt");
	REG_PROC(hscan, "rt");
	REG_PROC(hrscan, "rt");
	REG_PROC(hkeys, "rt");
	REG_PROC(hvals, "rt");
	REG_PROC(hlist, "rt");
	REG_PROC(hrlist, "rt");
	REG_PROC(hexists, "rt");
	REG_PROC(multi_hexists, "rt");
	REG_PROC(multi_hsize, "rt");
	REG_PROC(multi_hget, "rt");
	REG_PROC(multi_hset, "wt");
	REG_PROC(multi_hdel, "wt");

	// because zrank may be extremly slow, execute in a seperate thread
	REG_PROC(zrank, "rt");
	REG_PROC(zrrank, "rt");
	REG_PROC(zrange, "rt");
	REG_PROC(zrrange, "rt");
	REG_PROC(zsize, "rt");
	REG_PROC(zget, "rt");
	REG_PROC(zset, "wt");
	REG_PROC(zdel, "wt");
	REG_PROC(zincr, "wt");
	REG_PROC(zdecr, "wt");
	REG_PROC(zclear, "wt");
	REG_PROC(zscan, "rt");
	REG_PROC(zrscan, "rt");
	REG_PROC(zkeys, "rt");
	REG_PROC(zlist, "rt");
	REG_PROC(zrlist, "rt");
	REG_PROC(zcount, "rt");
	REG_PROC(zsum, "rt");
	REG_PROC(zavg, "rt");
	REG_PROC(zremrangebyrank, "wt");
	REG_PROC(zremrangebyscore, "wt");
	REG_PROC(zexists, "rt");
	REG_PROC(multi_zexists, "rt");
	REG_PROC(multi_zsize, "rt");
	REG_PROC(multi_zget, "rt");
	REG_PROC(multi_zset, "wt");
	REG_PROC(multi_zdel, "wt");
	REG_PROC(zpop_front, "wt");
	REG_PROC(zpop_back, "wt");

	REG_PROC(multi_sset, "wt");
	REG_PROC(multi_sdel, "wt");
	REG_PROC(ssize, "rt");
	REG_PROC(sismember, "rt");
	REG_PROC(smembers, "rt");

	REG_PROC(qsize, "rt");
	REG_PROC(qfront, "rt");
	REG_PROC(qback, "rt");
	REG_PROC(qpush, "wt");
	REG_PROC(qpush_front, "wt");
	REG_PROC(qpush_back, "wt");
	REG_PROC(qpop, "wt");
	REG_PROC(qpop_front, "wt");
	REG_PROC(qpop_back, "wt");
	REG_PROC(qtrim_front, "wt");
	REG_PROC(qtrim_back, "wt");
	REG_PROC(qfix, "wt");
	REG_PROC(qclear, "wt");
	REG_PROC(qlist, "rt");
	REG_PROC(qrlist, "rt");
	REG_PROC(qslice, "rt");
	REG_PROC(qrange, "rt");
	REG_PROC(qget, "rt");
	REG_PROC(qset, "wt");
	REG_PROC(qltrim, "wt");

	REG_PROC(flushdb, "wt");

	REG_PROC(dump, "w");
	REG_PROC(dump_slot, "w");
	REG_PROC(sync140, "b");
	REG_PROC(info, "r");
	REG_PROC(version, "r");
	REG_PROC(dbsize, "rt");
	// doing compaction in a reader thread, because we have only one
	// writer thread(for performance reason); we don't want to block writes
	REG_PROC(compact, "rt");

	REG_PROC(ignore_key_range, "r");
	REG_PROC(get_key_range, "r");
	REG_PROC(get_kv_range, "r");
	REG_PROC(set_kv_range, "r");

	REG_PROC(cluster_add_kv_node, "r");
	REG_PROC(cluster_del_kv_node, "r");
	REG_PROC(cluster_kv_node_list, "r");
	REG_PROC(cluster_set_kv_range, "r");
	REG_PROC(cluster_set_kv_status, "r");
	REG_PROC(cluster_migrate_kv_data, "r");

	/* migration */
	REG_PROC(migrate_slot, "rt");
	REG_PROC(migrate, "b");
	REG_PROC(flag_migrating, "rt");
	REG_PROC(flag_importing, "rt");
	REG_PROC(flag_normal, "rt");
	REG_PROC(migrate_result, "rt");
	REG_PROC(slot_premigrating, "rt");
	REG_PROC(slot_postmigrating, "rt");
	REG_PROC(slot_preimporting, "rt");
	REG_PROC(slot_postimporting, "rt");
	REG_PROC(asking, "rt");
	REG_PROC(set_slot, "w");
	REG_PROC(unset_slot, "w");
	REG_PROC(type, "rt");

	/* testing */
	REG_PROC(lock_key, "w");
	REG_PROC(unlock_key, "w");
	REG_PROC(key_slot, "r");

	/* global read lock */
	REG_PROC(lock_db_with_read_lock, "w");
	REG_PROC(unlock_db, "w");

	/* replicate */
	REG_PROC(show_slave_status, "rt");
	REG_PROC(change_master_to, "rt");
	REG_PROC(start_slave, "rt");
	REG_PROC(stop_slave, "rt");

	/* binlog */
	REG_PROC(purge_logs_to, "rt");
	REG_PROC(purge_logs_before, "rt");

	/* configuration*/
	REG_PROC(config, "rt");

	/* others */
	REG_PROC(client_pause, "w");
}

SSDBServer::SSDBServer(SSDB *ssdb, SSDB *meta, Config *conf,
	NetworkServer *net, SSDB_BinLog *binlog){
	this->ssdb = (SSDBImpl *)ssdb;
	this->meta = meta;
	this->binlog = binlog;
	this->global_read_lock = false;
	this->config = conf;

	net->data = this;
	this->net = net;
	this->reg_procs(net);

	int sync_speed = conf->get_num("replication.sync_speed");

	backend_dump = new BackendDump(this->ssdb);
	backend_sync = new BackendSync(this, this->ssdb, sync_speed);
	expiration = new ExpirationHandler(this->ssdb, binlog);

	/* create ClusterState and initilize Cluster */
	{
		local_tag = conf->get_str("server.tag");
		local_port = conf->get_num("server.port");
		const char *ip = conf->get_str("server.ip");
		if(ip == NULL || local_ip[0] == '\0'){
			local_ip = "127.0.0.1";
		} else {
			local_ip = ip;
		}
		cluster = new Cluster(this);
		if(cluster->init() == -1){
			log_fatal("cluster init failed!");
			exit(1);
		}

		/* get cluster config and init */
		ssdb_cluster = new SSDBCluster(this);
		if(ssdb_cluster->init() != 0) {
			log_fatal("ssdb cluster init failed!");
			exit(1);
		}
	}

	{ // slaves
		/*const Config *repl_conf = conf.get("replication");
		if(repl_conf != NULL){
			std::vector<Config *> children = repl_conf->children;
			for(std::vector<Config *>::iterator it = children.begin(); it != children.end(); it++){
				Config *c = *it;
				if(c->key != "slaveof"){
					continue;
				}
				std::string ip = c->get_str("ip");
				int port = c->get_num("port");
				if(ip == "" || port <= 0 || port > 65535){
					continue;
				}
				bool is_mirror = false;
				std::string type = c->get_str("type");
				if(type == "mirror"){
					is_mirror = true;
				}else{
					type = "sync";
					is_mirror = false;
				}

				std::string id = c->get_str("id");

				log_info("slaveof: %s:%d, type: %s", ip.c_str(), port, type.c_str());
				Slave *slave = new Slave(ssdb, meta, ip.c_str(), port, is_mirror);
				if(!id.empty()){
					slave->set_id(id);
				}
				slave->auth = c->get_str("auth");
				slave->start();
				slaves.push_back(slave);
			}
		} */

		// init slave
		this->slave = new Slave(this, meta, local_port);
		this->slave->init();

		int skip_slave_start = conf->get_num("rpl.skip_slave_start");
		if (!this->slave->mi->ip.empty()
			&& this->slave->mi->port > 0
			&& !skip_slave_start) {
			this->slave->start();
		}

		/* don't start expiration if is slave or in migrating */
		int16_t slot;
		int ret = ssdb_cluster->get_slot_migrating(&slot);
		if (ret != 0) {
			log_fatal("get slot migrating failed");
			exit(1);
		}
		if (slot < 0 && this->slave->mi->ip.empty()) {
			expiration->start();
		}
	}

	// load kv_range
	int ret = this->get_kv_range(&this->kv_range_s, &this->kv_range_e);
	if(ret == -1){
		log_fatal("load key_range failed!");
		exit(1);
	}
	log_info("key_range.kv: \"%s\", \"%s\"",
		str_escape(this->kv_range_s).c_str(),
		str_escape(this->kv_range_e).c_str()
		);
}

SSDBServer::~SSDBServer(){
	std::vector<Slave *>::iterator it;
	for(it = slaves.begin(); it != slaves.end(); it++){
		Slave *slave = *it;
		slave->stop();
		delete slave;
	}

	delete backend_dump;
	delete backend_sync;
	delete expiration;
	delete cluster;
	if (binlog) {
		delete binlog;
	}
	if (slave) {
		delete slave;
	}

	log_debug("SSDBServer finalized");
}

int SSDBServer::set_kv_range(const std::string &start, const std::string &end){
	/*Transaction trans(meta, std::string("key_range"));
	if(meta->hset("key_range", "kv_s", start, trans) == -1){
		return -1;
	}
	if(meta->hset("key_range", "kv_e", end, trans) == -1){
		return -1;
	}

	kv_range_s = start;
	kv_range_e = end;*/
	return 0;
}

int SSDBServer::get_kv_range(std::string *start, std::string *end){
	/*if(meta->hget("key_range", "kv_s", start) == -1){
		return -1;
	}
	if(meta->hget("key_range", "kv_e", end) == -1){
		return -1;
	}*/
	return 0;
}

bool SSDBServer::in_kv_range(const Bytes &key){
	/*if((this->kv_range_s.size() && this->kv_range_s >= key)
		|| (this->kv_range_e.size() && this->kv_range_e < key))
	{
		return false;
	}*/
	return true;
}

bool SSDBServer::in_kv_range(const std::string &key){
	/*if((this->kv_range_s.size() && this->kv_range_s >= key)
		|| (this->kv_range_e.size() && this->kv_range_e < key))
	{
		return false;
	}*/
	return true;
}

/*********************/

int proc_purge_logs_to(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	if (serv->binlog) {
		if (req.size() == 2) {
			std::string to(req[1].data(), req[1].size());
			serv->binlog->purge_logs(to);
		} else {
			resp->push_back("client error");
			return 0;
		}
	}
	resp->push_back("ok");
	return 0;
}

int proc_purge_logs_before(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;

	if (req.size() != 2) {
		resp->push_back("client error");
		return 0;
	}

	if (serv->binlog) {
		struct tm t;
		time_t time;

		std::string date(req[1].data(), req[1].size());
		if (strptime(date.c_str(), "%Y-%m-%d %H:%M:%S", &t) == NULL) {
			resp->push_back("invalid date format.");
			return 0;
		}

		time = mktime(&t);
		serv->binlog->purge_logs_before_date(time);
	}

	resp->push_back("ok");
	return 0;
}

int proc_flushdb(NetworkServer *net, Link *link, const Request &req, Response *resp){
	/*SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_READ_ONLY;

	if(serv->slaves.size() > 0 || serv->backend_sync->stats().size() > 0){
		resp->push_back("error");
		resp->push_back("flushdb is not allowed when replication is in use!");
		return 0;
	}

	serv->backend_sync->reset();

	serv->ssdb->flushdb();

	if (serv->binlog->reset() != 0) {
		log_error("reset binlog failed");
		resp->push_back("error");
		resp->push_back("reset binlog failed");
		return 0;
	}

	resp->push_back("ok");*/
	resp->push_back("error");
	resp->push_back("deprecated");
	return 0;
}

int proc_dump(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	if(serv->backend_dump->running()) {
		resp->push_back("error");
		resp->push_back("dumping");
		return 0;
	}

	int ret = serv->backend_dump->proc(req[1]);
	if(ret != 1) {
		resp->push_back("error");
		resp->push_back("start dump failed");
		return 0;
	}

	resp->push_back("ok");
	return 0;
}

int proc_dump_slot(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	if(serv->backend_dump->running()) {
		resp->push_back("error");
		resp->push_back("dumping");
		return 0;
	}

	int16_t slot = req[1].Int();
	if(slot < 0 || slot >= CLUSTER_SLOTS) {
		resp->push_back("error");
		resp->push_back("invalidate slot");
		return 0;
	}

	int ret = serv->backend_dump->proc(slot);
	if(ret != 1) {
		resp->push_back("error");
		resp->push_back("start dump failed");
		return 0;
	}

	resp->push_back("ok");
	return 0;
}

int proc_sync140(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->backend_sync->proc(link);
	return PROC_BACKEND;
}

int proc_asking(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	link->asking = true;
	resp->push_back("ok");
	return 0;
}

/* migrate_slot [slot] [ip] [port] [timeout] [speed]*/
int proc_migrate_slot(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(6);
	int16_t slot = req[1].Int();
	std::string ip = req[2].String();
	int port = req[3].Int();
	int64_t timeout_ms = req[4].Int64();
	int64_t speed = req[5].Int64();
	if(speed <= 0) {
		speed = 1;
	}
	if(slot <= 0 && slot >= CLUSTER_SLOTS) {
		resp->push_back("error");
		resp->push_back("invalid slot");
		log_warn("invalid slot %d", slot);
		return 0;
	}

	ReadLockGuard<RWLock> guard(serv->ssdb_cluster->get_state_lock(slot));
	int16_t flag_slot = -1;
	int ret = serv->ssdb_cluster->get_slot_migrating(&flag_slot);
	if(ret != 0) {
		log_warn("get slot migrating failed");
		resp->push_back("error");
		resp->push_back("server inner error");
		return 0;
	}
	if(flag_slot != slot) {
		log_warn("migrate slot %d with incompeleted migration at slot %d", slot, flag_slot);
		resp->push_back("error");
		resp->push_back("incompeleted migration");
		return 0;
	} else {
		serv->ssdb_cluster->migrate_slot(link, resp, slot, ip, port, timeout_ms, speed);
		log_debug("migrate quite");
		return 0;
	}
}

int proc_migrate(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	/* inner command, don't check vaildation */
	int16_t slot = KEY_HASH_SLOT(req[1]);
	log_debug("sync_key: %s slot: %d", req[1].String().c_str(), slot);
	ReadLockGuard<RWLock> guard(serv->ssdb_cluster->get_state_lock(slot));
	int flag = 0;
	int ret = serv->ssdb_cluster->test_slot_importing(slot, &flag);
	if(ret != 0) {
		log_warn("test slot importing failed");
		resp->push_back("-");
		return 0;
	}
	if(flag) {
		serv->ssdb_cluster->import_slot(link, req[1].String());
		return PROC_BACKEND;
	} else {
		log_warn("importing flag not set, reject impoting slot %d", slot);
		resp->push_back("-");
		return 0;
	}
}

int proc_slot_premigrating(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	/* check master */
	if(!serv->slave->mi->ip.empty()) {
		resp->push_back("error");
		resp->push_back("can't set migrating flag to a slave");
		return 0;
	}

	int16_t slot = req[1].Int();
	WriteLockGuard<RWLock> guard(serv->ssdb_cluster->get_state_lock(slot));
	int flag = 0;
	int ret = serv->ssdb_cluster->test_slot_importing(slot, &flag);
	if(ret != 0) {
		log_warn("test slot importing failed");
		resp->push_back("error");
		resp->push_back("server inner error");
		return 0;
	}

	if(flag) {
		resp->push_back("error");
		resp->push_back("slot is in importing");
		log_warn("slot %d is importing, set slot migrating failed", slot);
	} else {
		int16_t migrating_slot = 0;
		int ret = serv->ssdb_cluster->get_slot_migrating(&migrating_slot);
		if(ret != 0) {
			log_warn("get slot migrating failed");
			resp->push_back("error");
			resp->push_back("server inner error");
			return 0;
		}
		if(migrating_slot == -1) {
			int ret = serv->ssdb_cluster->set_slot_migrating(slot);
			if(ret != -1) {
				resp->push_back("ok");
				/* disable expiration while migrating */
				serv->expiration->stop();
				return 0;
			} else {
				resp->push_back("error");
				resp->push_back("server inner error");
				log_error("set slot %d migrating failed", slot);
			}
		} else {
			resp->push_back("error");
			resp->push_back("slot is in migrating");
			log_warn("slot %d is migrating, set slot migrating failed", slot);
		}
	}
	return 0;
}

int proc_slot_postmigrating(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	/* check master */
	if(!serv->slave->mi->ip.empty()) {
		resp->push_back("error");
		resp->push_back("can't set migrating flag to a slave");
		return 0;
	}

	int16_t slot = req[1].Int();
	WriteLockGuard<RWLock> guard(serv->ssdb_cluster->get_state_lock(slot));
	int16_t slot_migrating  = 0;
	int ret = serv->ssdb_cluster->get_slot_migrating(&slot_migrating);
	if(ret == -1) {
		resp->push_back("error");
		resp->push_back("get slot migrating failed");
		log_error("get slot migrating failed");
		return 0;
	}
	if(slot != slot_migrating) {
		resp->push_back("ok");
		return 0;
	}

	/* set topo */
	ret = serv->ssdb_cluster->unset_slot(slot);
	if(ret == -1) {
		resp->push_back("error");
		resp->push_back("unset slot failed");
		log_error("unset slot %d failed", slot);
		return 0;
	}

	/* clean migration flag */
	ret = serv->ssdb_cluster->unset_slot_migrating();
	if(ret == -1) {
		resp->push_back("error");
		resp->push_back("server inner error");
		log_error("unset slot %d migrating failed", slot);
	} else if (ret == 1){
		resp->push_back("ok");
		/* start expiration after migrating */
		serv->expiration->start();
	}
	return 0;
}

int proc_slot_preimporting(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	/* check master */
	if(!serv->slave->mi->ip.empty()) {
		resp->push_back("error");
		resp->push_back("can't set importing flag to a slave");
		return 0;
	}

	int16_t slot = req[1].Int();
	WriteLockGuard<RWLock> guard(serv->ssdb_cluster->get_state_lock(slot));
	int16_t migrating_slot = 0;
	int ret = serv->ssdb_cluster->get_slot_migrating(&migrating_slot);
	if(ret != 0) {
		log_warn("get slot migrating failed");
		resp->push_back("error");
		return 0;
	}
	if(migrating_slot != slot) {
		int ret = serv->ssdb_cluster->set_slot_importing(slot);
		if(ret == 0) {
			resp->push_back("ok");
			return 0;
		}
	}
	resp->push_back("error");
	log_warn("slot %d is migrating, set slot importing failed", slot);
	return 0;
}

int proc_slot_postimporting(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	/* check master */
	if(!serv->slave->mi->ip.empty()) {
		resp->push_back("error");
		resp->push_back("can't set importing flag to a slave");
		return 0;
	}

	int16_t slot = req[1].Int();
	WriteLockGuard<RWLock> guard(serv->ssdb_cluster->get_state_lock(slot));
	int flag = 0;
	int ret = serv->ssdb_cluster->test_slot_importing(slot, &flag);
	if(ret == -1) {
		resp->push_back("error");
		resp->push_back("server get slot importing failed");
		log_error("test slot %d importing failed", slot);
		return 0;
	}
	if(!flag) {
		resp->push_back("ok");
		return 0;
	}
	ret = serv->ssdb_cluster->set_slot(slot);
	if(ret == -1) {
		resp->push_back("error");
		resp->push_back("set slot failed");
		log_error("set slot %d failed", slot);
		return 0;
	}

	ret = serv->ssdb_cluster->unset_slot_importing(slot);
	if(ret == -1) {
		resp->push_back("error");
		resp->push_back("unset slot importing failed");
		log_error("unset slot %d importing failed", slot);
	} else {
		resp->push_back("ok");
	}
	return 0;
}

/**
 * flag migrate 'slot' to 'ip:port'
 * flag_migrating [slot] [ip] [port]
 **/
int proc_flag_migrating(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);
	int16_t slot = req[1].Int();
	std::string ip = req[2].String();
	int port = req[3].Int();
	int ret = serv->ssdb_cluster->flag_migrating(slot, ip, port);
	if(ret == 0) {
		resp->push_back("ok");
	} else {
		resp->push_back("error");
	}
	return 0;
}

/**
 * flag import 'slot' from 'ip:port'
 * flag_importing [slot] [ip] [port]
 **/
int proc_flag_importing(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);
	int16_t slot = req[1].Int();
	std::string ip = req[2].String();
	int port = req[3].Int();
	int ret = serv->ssdb_cluster->flag_importing(slot, ip, port);
	if(ret == 0) {
		resp->push_back("ok");
	} else {
		resp->push_back("error");
	}
	return 0;
}

/**
 * flag_normal [slot]
 **/
int proc_flag_normal(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	int16_t slot = req[1].Int();
	if(slot < 0) {
		resp->push_back("error");
		resp->push_back("invalidate slot");
		return 0;
	}

	int ret = serv->ssdb_cluster->flag_normal(slot);
	if(ret == 0) {
		resp->push_back("ok");
	} else {
		resp->push_back("error");
	}
	return 0;
}

/* NOTIC: currently, we don't disable this interface of slaves for compatibility */
/* TODO: disable for slave */
int proc_set_slot(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	int16_t slot = req[1].Int();
	if(slot < 0 || slot >= CLUSTER_SLOTS) {
		resp->push_back("error");
		resp->push_back("slot out of range");
		return 0;
	}

	int ret = serv->ssdb_cluster->set_slot(slot);
	if(ret == -1) {
		resp->push_back("error");
		resp->push_back("server inner error");
		return 0;
	}

	resp->push_back("ok");
	return 0;
}

int proc_unset_slot(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	int16_t slot = req[1].Int();
	if(slot < 0 || slot >= CLUSTER_SLOTS) {
		resp->push_back("error");
		resp->push_back("slot out of range");
		return 0;
	}

	int ret = serv->ssdb_cluster->unset_slot(slot);
	if(ret == -1) {
		resp->push_back("error");
		resp->push_back("server inner error");
		return 0;
	}

	resp->push_back("ok");
	return 0;
}


int proc_migrate_result(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	resp->push_back(serv->ssdb_cluster->get_migrate_result());
	return 0;
}

int proc_compact(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->ssdb->compact();
	resp->push_back("ok");
	return 0;
}

int proc_ignore_key_range(NetworkServer *net, Link *link, const Request &req, Response *resp){
	link->ignore_key_range = true;
	resp->push_back("ok");
	return 0;
}

// get kv_range, hash_range, zset_range, list_range
int proc_get_key_range(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	std::string s, e;
	int ret = serv->get_kv_range(&s, &e);
	if(ret == -1){
		resp->push_back("error");
	}else{
		resp->push_back("ok");
		resp->push_back(s);
		resp->push_back(e);
		// TODO: hash_range, zset_range, list_range
	}
	return 0;
}

int proc_get_kv_range(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	std::string s, e;
	int ret = serv->get_kv_range(&s, &e);
	if(ret == -1){
		resp->push_back("error");
	}else{
		resp->push_back("ok");
		resp->push_back(s);
		resp->push_back(e);
	}
	return 0;
}

int proc_set_kv_range(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(req.size() != 3){
		resp->push_back("client_error");
	}else{
		serv->set_kv_range(req[1].String(), req[2].String());
		resp->push_back("ok");
	}
	return 0;
}

int proc_dbsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	uint64_t size = serv->ssdb->leveldbfilesize();
	resp->push_back("ok");
	resp->push_back(str(size));
	return 0;
}

int proc_version(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	resp->push_back(SSDB_VERSION);
	return 0;
}

int proc_info(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	resp->push_back("ok");
	resp->push_back("# ssdb-server");
	resp->push_back("version:" SSDB_VERSION);

	{
		resp->push_back("links:" + str(net->link_count));
	}

	{
		resp->push_back("total_calls:" + str(net->total_calls));
	}

	{
		resp->push_back("ops:" + str(net->ops));
	}

	{
		uint64_t size = serv->ssdb->leveldbfilesize();
		resp->push_back("dbsize:" + str(size));
	}

	{
		int running = serv->expiration->running();
		resp->push_back("expiration:" + str(running));
	}

	{
		if(serv->global_read_lock) {
			resp->push_back("readonly:1");
		} else {
			resp->push_back("readonly:0");
		}
	}

	resp->push_back("\n");
	resp->push_back("# replication");

	{
		std::vector<std::string> syncs = serv->backend_sync->stats();
		std::vector<std::string>::iterator it;
		for(it = syncs.begin(); it != syncs.end(); it++){
			std::string s = *it;
			resp->push_back(s + "\n");
		}
	}

	{
		if (serv->slave) {
			resp->push_back(serv->slave->stats() + "\n");
		}
	}

	resp->push_back("\n");
	resp->push_back("# slot");
	{
		std::vector<int> range = serv->ssdb_cluster->slot_range();
		if(range.size() == 0) {
			resp->push_back("none");
		} else {
			for(int i = 0; i+1 < range.size(); i+=2) {
				std::string s = "[" + str(range[i]) + "-" + str(range[i+1]) + "]";
				resp->push_back(s);
			}
		}

		int16_t slot_migrating = 0;
		serv->ssdb_cluster->get_slot_migrating(&slot_migrating);
		if(slot_migrating > 0) {
			resp->push_back("migrating:" + str(slot_migrating));
		}

		for(int i = 0; i < CLUSTER_SLOTS; ++i) {
			int flag = 0;
			serv->ssdb_cluster->test_slot_importing(i, &flag);
			if(flag) {
				resp->push_back("importing:"+str(i));
				break;
			}
		}
	}

	resp->push_back("\n");
	resp->push_back("# Keyspace");
	{
		uint64_t nexpires = serv->expiration->expires();
		resp->push_back("expires:" + str(nexpires));
	}

	if(req.size() == 1 || req[1] == "leveldb"){
		resp->push_back("# levedb");
		std::vector<std::string> tmp = serv->ssdb->info();
		for(int i=0; i<(int)tmp.size(); i++){
			std::string block = tmp[i];
			resp->push_back(block);
		}
	}

	if(req.size() > 1 && req[1] == "cmd"){
		proc_map_t::iterator it;
		for(it=net->proc_map.begin(); it!=net->proc_map.end(); it++){
			Command *cmd = it->second;
			resp->push_back("cmd." + cmd->name);
			char buf[128];
			snprintf(buf, sizeof(buf), "calls: %" PRIu64 "\ttime_wait: %.0f\ttime_proc: %.0f",
				cmd->calls, cmd->time_wait, cmd->time_proc);
			resp->push_back(buf);
		}
		resp->push_back("\n");
	}

	return 0;
}

/* testing */
int proc_lock_key(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string key = req[1].String();
	KeyLock &key_lock = serv->ssdb_cluster->get_key_lock(key);
	WriteLockGuard<KeyLock> key_guard(key_lock);
	key_lock.add_key(key);
	resp->reply_bool(1);
	return 0;
}

int proc_unlock_key(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string key = req[1].String();
	KeyLock &key_lock = serv->ssdb_cluster->get_key_lock(key);
	WriteLockGuard<KeyLock> key_guard(key_lock);
	key_lock.del_key(key);
	resp->reply_bool(1);
	return 0;
}

int proc_key_slot(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	CHECK_NUM_PARAMS(2);

	int16_t slot = KEY_HASH_SLOT(req[1]);
	resp->reply_int(0, slot);
	return 0;
}

int proc_lock_db_with_read_lock(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->global_read_lock = true;
	resp->reply_bool(1);
	return 0;
}

int proc_unlock_db(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;

	/* enable as a master, start expiration */
	if (serv->slave->mi->ip.empty()) {
		serv->global_read_lock = false;
		serv->expiration->start();
		resp->push_back("ok");
		return 0;
	}

	/* slave cannot enable write */
	resp->push_back("error");
	resp->push_back("Err cannot enable write for slave");
	return 0;
}

int proc_show_slave_status(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;

	// issue: last_seq and binlog_seq out of sync
	serv->ssdb->lock_db();

	std::string master_ip = serv->slave->mi->ip;
	std::string master_port = str(serv->slave->mi->port);
	std::string last_seq = str(serv->slave->mi->last_seq);
	std::string last_key = serv->slave->mi->last_key;
	std::string binlog_seq = str(serv->binlog!=NULL ? serv->binlog->get_last_seq() : 0);
	std::string running = str(serv->slave->running);

	resp->push_back("ok");
	resp->push_back("ssdb-server");
	resp->push_back("master-ip");
	resp->push_back(master_ip);
	resp->push_back("master-port");
	resp->push_back(master_port);
	resp->push_back("last-seq");
	resp->push_back(last_seq);
	resp->push_back("last-key");
	resp->push_back(last_key);
	resp->push_back("binlog-seq");
	resp->push_back(binlog_seq);
	resp->push_back("running");
	resp->push_back(running);

	serv->ssdb->unlock_db();

	return 0;
}

/* change_master to ip port last_seq last_key */
int proc_change_master_to(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;

	if (serv->slave->running) {
		resp->push_back("error");
		resp->push_back("slave is running");
		return 0;
	}

	/* check impoting */
	for (int i = 0; i < CLUSTER_SLOTS; ++i) {
		int flag = 0;
		int ret = serv->ssdb_cluster->test_slot_importing(i, &flag);
		if (ret != 0) {
			resp->push_back("error");
			resp->push_back("server inner error");
			log_error("test slot impoting failed.");
			return 0;
		}
		if (flag) {
			resp->push_back("error");
			resp->push_back("can't change master while importing.");
			return 0;
		}
	}
	/* check migrating */
	{
		int16_t slot = 0;
		int ret = serv->ssdb_cluster->get_slot_migrating(&slot);
		if (ret != 0) {
			resp->push_back("error");
			resp->push_back("server inner error");
			log_error("get migrating slot failed.");
			return 0;
		}
		if (slot >= 0) {
			resp->push_back("error");
			resp->push_back("can't change master wihle migrating.");
			return 0;
		}
	}

	CHECK_NUM_PARAMS(5);

	std::string ip = req[1].String();
	int port = req[2].Int();
	uint64_t last_seq = req[3].Uint64();
	std::string last_key = req[4].String();

	char ip_raw[32];
	int ret = get_local_ip(ip_raw, sizeof(ip_raw));
	if (ret != 0) {
		log_warn("get local ip failed");
	}
	if(port == serv->local_port &&
			 (strcmp(ip_raw, ip.c_str()) == 0 ||
			  serv->local_ip == ip)){
		log_warn("can't slave of myself");
		resp->push_back("error");
		resp->push_back("cannot slave of myself");
		return 0;
	}

	serv->slave->mi->ip = ip;
	serv->slave->mi->port = port;
	serv->slave->mi->last_seq = last_seq;
	serv->slave->mi->last_key = last_key;
	if (req.size() > 5) {
		std::string auth = req[5].String();
		serv->slave->mi->auth = auth;
	}

	if (serv->slave->mi->save() == -1) {
		resp->push_back("error");
		resp->push_back("internal error");
	} else {
		WriteLockGuard<RWLock>(serv->config_lock);
		serv->config->set("replication.slaveof.ip", ip.c_str());
		serv->config->set("replication.slaveof.port", req[2].String().c_str());
		resp->push_back("ok");
		resp->push_back("save master info success");
		log_info("change master to %s:%d", ip.c_str(), port);
	}

	return 0;
}

int proc_start_slave(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	if (serv->slave->running) {
		resp->push_back("error");
		resp->push_back("slave is running");
		return 0;
	}

	if (serv->slave->mi->ip.empty() || serv->slave->mi->port <= 0) {
		resp->push_back("error");
		resp->push_back("no master specified");
		return 0;
	}

	serv->expiration->stop();
	serv->slave->start();
	resp->reply_bool(1);

	return 0;
}

int proc_stop_slave(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	if (!serv->slave->running) {
		resp->push_back("error");
		resp->push_back("slave thread is not running");
		return 0;
	}

	if (req.size() == 1) {
		serv->slave->stop();
	} else {
		int64_t target_seq = req[1].Int64();
		int64_t last_seq = serv->slave->mi->last_seq;
		if (last_seq <= target_seq) {
			serv->slave->failover_seq = target_seq;
			serv->slave->stop();
		} else {
			resp->push_back("error");
			resp->push_back("last seq is greater than target seq");
			return 0;
		}
	}

	/* load slot map into cache */
	int ret = serv->ssdb_cluster->init_slot();
	if (ret == -1) {
		resp->push_back("error");
		resp->push_back("init slot failed");
		log_error("init slot failed");
		return 0;
	}

	resp->reply_bool(1);
	return 0;
}

/* config get [key] */
/* config rewrite [path] */
int proc_config(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	std::string action = req[1].String();
	if(action == "set") {
		//TODO consistance check
	///* disable currently*/
		/*CHECK_NUM_PARAMS(4);
		WriteLockGuard<RWLock>(serv->config_lock);
		serv->config->set(req[2].String().c_str(), req[3].String().c_str());
		std::string val("OK");
		resp->reply_get(1, &val);*/
		resp->push_back("error");
		return 0;
	} else if (action == "get") {
		CHECK_NUM_PARAMS(3);
		ReadLockGuard<RWLock>(serv->config_lock);
		std::string val = serv->config->get_str(req[2].String().c_str());
		resp->reply_get(1, &val);
		return 0;
	} else if (action == "rewrite") {
		WriteLockGuard<RWLock>(serv->config_lock);
		int ret = 0;
		if(req.size() >= 3) {
			ret = serv->config->rewrite(req[2].String().c_str());
		} else {
			ret = serv->config->rewrite(serv->conf_file.c_str());
		}
		if (ret == 0) {
			std::string val("OK");
			resp->reply_get(1, &val);
			return 0;
		}
	}
	resp->push_back("error");
	return 0;
}

int proc_client_pause(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	CHECK_NUM_PARAMS(2);

	int64_t spam = req[1].Int64();
	log_info("proc_client_pause");
	net->clients_paused = 1;
	net->clients_pause_end_time = time_ms() + spam;
	resp->push_back("ok");
	return 0;
}

int proc_type(NetworkServer *net, Link *link, const Request &req, Response *resp) {
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	uint64_t version;
	char op;
	int exists;
	CHECK_META(req[1], op, version, exists);

	resp->push_back("ok");
	if(!exists) {
		resp->push_back("none");
		return 0;
	}

	switch(op) {
		case DataType::KV:
			resp->push_back("string");
			break;
		case DataType::HASH:
			resp->push_back("hash");
			break;
		case DataType::SET:
			resp->push_back("set");
			break;
		case DataType::ZSET:
			resp->push_back("zset");
			break;
		case DataType::QUEUE:
			resp->push_back("list");
			break;
		default:
			resp->push_back("unknow");
	}
	return 0;
}
