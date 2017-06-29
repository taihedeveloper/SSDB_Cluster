/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_SLAVE_H_
#define SSDB_SLAVE_H_

#include <stdint.h>
#include <string>
#include <pthread.h>
#include <vector>
#include "ssdb/ssdb_impl.h"
#include "ssdb/logevent.h"
#include "net/link.h"
#include "rpl_mi.h"

class SSDBServer;

class Slave{
private:
	uint64_t copy_count;
	uint64_t sync_count;

	SSDBServer *serv;
	SSDB *meta;
	Link *link;
	bool is_mirror;
	char log_type;
	int server_port;

	static const int DISCONNECTED = 0;
	static const int INIT = 1;
	static const int COPY = 2;
	static const int SYNC = 4;
	int status;

	void migrate_old_status();

	std::string status_key();
	void load_status();
	void save_status();

	static void* _run_thread(void *arg);

	int proc(const std::vector<Bytes> &req);
	int proc(const LogEvent &event, const std::vector<Bytes> &req);

	int proc_noop(uint64_t seq);
	int proc_noop(const LogEvent &event, const std::vector<Bytes> &req);

	int proc_copy(const LogEvent &event, const std::vector<Bytes> &req);

	int proc_sync(const LogEvent &event, const std::vector<Bytes> &req);

	unsigned int connect_retry;
	int connect();
	bool connected() { return link != NULL; }

public:
	std::string auth;
	int failover_seq;

	Slave(SSDBServer *serv, SSDB *meta, int serv_port);
	~Slave();
	void start();
	void stop();

	//void set_id(const std::string &id);
	std::string stats() const;

public:
	MasterInfo *mi;
	volatile bool running;
	volatile bool quit_thread;
	pthread_t thread_id;
	int version;

public:
	void init();
	void save_progress(bool include_last_key = false);

private:
	// KV
	int proc_k_set(const LogEvent &event);
	int proc_k_del(const LogEvent &event);
	int proc_k_incr(const LogEvent &event);
	int proc_k_decr(const LogEvent &event);
	int proc_k_expire(const LogEvent &event);
	int proc_k_expire_at(const LogEvent &event);
	int proc_k_setbit(const LogEvent &event);

	// HASH
	int proc_h_set(const LogEvent &event);
	int proc_h_del(const LogEvent &event);
	int proc_h_clear(const LogEvent &event);
	int proc_h_incr(const LogEvent &event);
	int proc_h_decr(const LogEvent &event);

	// ZSET
	int proc_z_set(const LogEvent &event);
	int proc_z_del(const LogEvent &event);
	int proc_z_clear(const LogEvent &event);
	int proc_z_incr(const LogEvent &event);
	int proc_z_decr(const LogEvent &event);
	int proc_z_pop_front(const LogEvent &event);
	int proc_z_pop_back(const LogEvent &event);

	// QUEUE
	int proc_q_push_front(const LogEvent &event);
	int proc_q_push_back(const LogEvent &event);
	int proc_q_pop_front(const LogEvent &event);
	int proc_q_pop_back(const LogEvent &event);
	int proc_q_fix(const LogEvent &event);
	int proc_q_clear(const LogEvent &event);
	int proc_q_set(const LogEvent &event);

	// SET
	int proc_s_set(const LogEvent &event);
	int proc_s_del(const LogEvent &event);
	int proc_s_clear(const LogEvent &event);

	// RAW
	int proc_raw(const LogEvent &event);

private:
	static int zpop(ZIterator *iter, SSDBServer *serv, const Bytes &name, Transaction &trans);
};

#endif
