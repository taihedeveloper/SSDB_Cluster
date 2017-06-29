/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_BINLOG_H_
#define SSDB_BINLOG_H_

#include <string>
#include <list>
#include <map>
#include <pthread.h>
#include "ssdb.h"
#include "logevent.h"
#include "log_reader_writer.h"
#include "../util/spin_lock.h"

#define SSDB_BINLOG_RESEVE_SEQ 0
#define SSDB_BINLOG_INIT_SEQ   1

class SSDB_BinLog {
private:
	static const std::string BINLOG_FILE_LIST;
	static const std::string BINLOG_LAST_SEQ;

private:
	SSDB *meta;
	std::string binlog_dir;
	bool sync_binlog;
	int next_file_num;

	std::list<std::string> files;
	std::map<std::string, uint64_t> files_min_seq;
	uint64_t last_seq;

	mutable RWLock rwlock;
	LogFile *active_log;
	LogWriter *writer;

	uint64_t max_binlog_size;
	uint64_t bytes_written;
	uint64_t active_log_size;

	uint64_t purge_logs_span;

	std::map<std::string, uint32_t> inuse_binlogs;

public:
	// update cond
	pthread_mutex_t mutex;
	pthread_cond_t cond;

private:
	void broadcast_update() {
		pthread_cond_broadcast(&this->cond);
	}
	void pre_write() {
		pthread_mutex_lock(&mutex);
	}
	void post_write() {
		broadcast_update();
		pthread_mutex_unlock(&mutex);
	}

	int clean();

public:
	SSDB_BinLog(SSDB *meta, const std::string &dir, uint64_t max_binlog_size=0, bool sync=false, time_t purge_span = 0);
	~SSDB_BinLog();

public:
	int recover();
	int new_file();
	int rotate();
	int stop();
	int reset();

	int write_impl(LogEvent *event);
	int write_impl(LogEventBatch *batch);

	int write(LogEvent *event);

	int write(char type, char cmd);
	int write(char type, char cmd, const Bytes &key, uint64_t ttl=0);
	int write(char type, char cmd, const Bytes &key, const Bytes &val, uint64_t ttl=0);

	int flush();
	int sync();

	void incr_inuse(const std::string &binlog);
	void decr_inuse(const std::string &binlog);
	bool inuse(const std::string &binlog);

	int purge_logs(const std::string &to_log, bool included=false);
	int purge_logs_before_date(time_t date);

public:
	uint64_t get_last_seq() const { return last_seq; }
	int incr_seq();

public:
	bool is_open() const { return active_log->fd >= 0; }
	uint64_t get_bytes_written() const { return bytes_written; }
	uint64_t get_active_log_size() const { return active_log_size; }

	std::string dir() const { return binlog_dir; }

	/*
	 * return the binlog contain the event seq==@seq,
	 * or empty string represents no such binlog.
	 */
	std::string find_binlog(uint64_t seq) const;
	std::string find_next(const std::string &curr);

public:
	bool is_active_log(const std::string &filename) const;

private:
	int load_last_seq();
	int save_last_seq();
	int check_rotate();
	void statistic(uint64_t bytes) {
		this->bytes_written += bytes;
		this->active_log_size += bytes;
	}

private:
	static std::string generate_name(int num);
	static int extract_file_num(const std::string &filename);

	int erase_befores(size_t idx);

private:
	// forbidden copy&&assignment
	SSDB_BinLog();
	const SSDB_BinLog &operator=(const SSDB_BinLog &r);
};

#endif
