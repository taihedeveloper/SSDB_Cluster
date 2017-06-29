/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_SERVER_H_
#define NET_SERVER_H_

#include "../include.h"
#include <string>
#include <map>

#include "fde.h"
#include "proc.h"
#include "worker.h"

#include "../util/spin_lock.h"

class Link;
class Config;
class IpFilter;
class Fdevents;

typedef std::map<int, Link *> link_dict_t;

class NetworkServer
{
private:
	int tick_interval;
	int status_report_ticks;

	//Config *conf;
	Link *serv_link;
	IpFilter *ip_filter;
	Fdevents *fdes;

	Link* accept_link();
	int proc_result(ProcJob *job, link_dict_t *ready_list);
	int proc_client_event(const Fdevent *fde, link_dict_t *ready_list);
	static void* _ops_timer_thread(void *arg);

	void proc(ProcJob *job);

	int num_readers;
	int num_writers;
	ProcWorkerPool *writer;
	ProcWorkerPool *reader;
	RWLock proc_mutex;

	NetworkServer();

protected:
	void usage(int argc, char **argv);

public:
	void *data;
	ProcMap proc_map;
	int link_count;
	uint64_t ops;
	uint64_t total_calls;
	static int clients_paused;
	static int64_t clients_pause_end_time;
	bool need_auth;
	std::string password;

	~NetworkServer();

	// could be called only once
	static NetworkServer* init(const char *conf_file, int num_readers=-1, int num_writers=-1);
	static NetworkServer* init(const Config &conf, int num_readers=-1, int num_writers=-1);
	void serve();
	void pause();
	void proceed();
};


#endif
