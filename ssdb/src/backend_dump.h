/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_BACKEND_DUMP_H_
#define SSDB_BACKEND_DUMP_H_

#include "include.h"
#include "ssdb/ssdb.h"
#include "net/link.h"

class BackendDump{
private:
	struct run_arg{
		BackendDump *backend;
		std::string pattern;
		int16_t slot;
	};
	static void* _dump_regex_thread(void *arg);
	static void* _dump_slot_thread(void *arg);
	SSDB *ssdb;
	pthread_t tid;
public:
	BackendDump(SSDB *ssdb);
	~BackendDump();
	int proc(const Bytes &pattern);
	int proc(int16_t slot);
	int running();
};

#endif
