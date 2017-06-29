/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_LOGEVENT_H_
#define SSDB_LOGEVENT_H_

#include <string>
#include <vector>
#include "../util/bytes.h"

/* total_size + tran_seq + type + cmd + ttl + key + [val] */
#define LOG_EVENT_HEAD_LEN (sizeof(uint32_t) + 2*sizeof(uint64_t) + 2)

class LogEvent {
private:
	std::string buf;
	Bytes _key;
	Bytes _val;

public:
	LogEvent() {}
	LogEvent(uint64_t seq, char type, char cmd);
	LogEvent(uint64_t seq, char type, char cmd, const Bytes &key, int64_t ttl=0);
	LogEvent(uint64_t seq, char type, char cmd, const Bytes &key, const Bytes &val, int64_t ttl=0);


public:
	uint32_t size() const;
	uint64_t seq() const;
	char type() const;
	char cmd() const;
	Bytes key() const;
	Bytes val() const;
	int64_t ttl() const;

	void clear() { buf.clear(); }

	std::string &repr() { return buf; }
	int inner_load();

	int load_head(const char *head, size_t len);
	int load_body(const char *body, size_t len);
	int load(const char *data, size_t len);
	int load(const Bytes &s);
/*
	int load(const std::string &s);
	int load(const leveldb::Slice &s);*/

public:
	static void pack32(std::string &s, uint32_t n);
	static void pack64(std::string &s, uint64_t n);
	static uint32_t unpack32(const char *buf);
	static uint64_t unpack64(const char *buf);
};

class LogEventBatch {
public:
	std::vector<LogEvent *> events;

public:
	LogEventBatch() {}
	~LogEventBatch() { clear(); }

public:
	void add_event(LogEvent *event) {
		events.push_back(event);
	}

	void clear() {
		for (size_t i = 0; i < events.size(); i++) {
			if (events[i]) {
				delete events[i];
				events[i] = NULL;
			}
		}

		events.clear();
	}
};

#endif
