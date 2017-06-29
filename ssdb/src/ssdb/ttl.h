/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_TTL_H_
#define SSDB_TTL_H_

#include "ssdb.h"
#include "binlog2.h"
#include "../util/thread.h"
#include "../util/sorted_set.h"
#include <string>

#define EXPIR_CON_DEGREE 3
#define EXPIR_CONCURRENT (1<<EXPIR_CON_DEGREE)

class ExpirationHandler
{
public:
	ExpirationHandler(SSDB *ssdb, SSDB_BinLog *binlog);
	~ExpirationHandler();

	// "In Redis 2.6 or older the command returns -1 if the key does not exist
	// or if the key exist but has no associated expire. Starting with Redis 2.8.."
	// I stick to Redis 2.6
	int64_t get_ttl(const Bytes &key, const leveldb::Snapshot *snapshot=NULL);
	// The caller must hold mutex before calling set/del functions
	int del_ttl(const Bytes &key);
	int set_ttl(const Bytes &key, int64_t ttl);
	void start();
	void stop();
	int running();
	uint64_t expires();

private:
	SSDB *ssdb;
	SSDB_BinLog *binlog;
	volatile bool thread_quit;
	Mutex mutexs[EXPIR_CONCURRENT];
	std::string list_name[EXPIR_CONCURRENT];
	int64_t first_timeout[EXPIR_CONCURRENT];
	SortedSet fast_keys[EXPIR_CONCURRENT];
	pthread_t tid;

	void expire_loop();
	static void* thread_func(void *arg);
	void load_expiration_keys_from_db(int idx, int num);

private:
	static uint32_t string_hash(const Bytes &s);
};

#endif
