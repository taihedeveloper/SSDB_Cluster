/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "logevent.h"
#include "const.h"
#include "../include.h"
#include "../util/log.h"
#include "../util/strings.h"
#include <map>
#include <stdlib.h>

LogEvent::LogEvent(uint64_t seq, char type, char cmd) {
	uint32_t total_size = LOG_EVENT_HEAD_LEN;
	buf.reserve(total_size);
	pack32(buf, total_size);
	pack64(buf, seq);
	buf.push_back(type);
	buf.push_back(cmd);
	pack64(buf, (uint64_t)0LL);
}

LogEvent::LogEvent(uint64_t seq, char type, char cmd, const Bytes &key, int64_t ttl) {
	uint32_t total_size = LOG_EVENT_HEAD_LEN + sizeof(uint32_t) + key.size();
	uint32_t key_size = key.size();
	buf.reserve(total_size);
	pack32(buf, total_size);
	pack64(buf, seq);
	buf.push_back(type);
	buf.push_back(cmd);
	pack64(buf, (uint64_t)ttl);
	pack32(buf, key_size);
	buf.append(key.data(), key.size());

	this->_key = Bytes(buf.data() + LOG_EVENT_HEAD_LEN + sizeof(uint32_t), key_size);
}

LogEvent::LogEvent(uint64_t seq, char type, char cmd, const Bytes &key, const Bytes &val, int64_t ttl) {
	uint32_t total_size = LOG_EVENT_HEAD_LEN + sizeof(uint32_t) + key.size()
							+ sizeof(uint32_t) + val.size();
	uint32_t key_size = key.size();
	uint32_t val_size = val.size();
	buf.reserve(total_size);
	pack32(buf, total_size);
	pack64(buf, seq);
	buf.push_back(type);
	buf.push_back(cmd);
	pack64(buf, (uint64_t)ttl);
	pack32(buf, key_size);
	buf.append(key.data(), key.size());
	pack32(buf, val_size);
	buf.append(val.data(), val.size());

	this->_key = Bytes(buf.data() + LOG_EVENT_HEAD_LEN + sizeof(uint32_t), key_size);
	this->_val = Bytes(buf.data() + LOG_EVENT_HEAD_LEN + sizeof(uint32_t) + key_size + sizeof(uint32_t), val_size);
}

uint32_t LogEvent::size() const {
	return unpack32(buf.data());
}

uint64_t LogEvent::seq() const {
	return unpack64(buf.data() + sizeof(uint32_t));
}

char LogEvent::type() const {
	return buf[sizeof(uint32_t) + sizeof(uint64_t)];
}

char LogEvent::cmd() const {
	return buf[sizeof(uint32_t) + sizeof(uint64_t) + 1];
}

int64_t LogEvent::ttl() const {
	return (int64_t)unpack64(buf.data() + LOG_EVENT_HEAD_LEN - sizeof(uint64_t));
}

Bytes LogEvent::key() const {
	return this->_key;
}

Bytes LogEvent::val() const {
	return this->_val;
}


int LogEvent::load(const Bytes &s){
	return load(s.data(), s.size());
}

/*
int LogEvent::load(const leveldb::Slice &s){
	return load(s.data(), s.size());
}

int LogEvent::load(const std::string &s){
	return load(s.data(), s.size());
}
*/

int LogEvent::load(const char *data, size_t size) {
	if (size < LOG_EVENT_HEAD_LEN) {
		log_error("invalid log event, too short");
		return -1;
	}

	buf.reserve(size);

	if (load_head(data, LOG_EVENT_HEAD_LEN) != 0) {
		log_error("load head failed");
		return -2;
	}

	if (size > LOG_EVENT_HEAD_LEN
			&& load_body(data+LOG_EVENT_HEAD_LEN, size-LOG_EVENT_HEAD_LEN) != 0) {
		log_error("load body failed");
		return -3;
	}

	if (inner_load() != 0) {
		log_error("inner load failed");
		return -4;
	}

	return 0;
}

#define CHECK_OUT_OF_SIZE(x) \
do { \
	if ((x) > end) { \
		return -1;\
	} \
} while(0)

int LogEvent::load_head(const char *head, size_t size) {
	if (size != LOG_EVENT_HEAD_LEN) {
		log_error("invalid log event head");
		return -1;
	}

	this->buf.assign(head, size);
	return 0;
}

int LogEvent::load_body(const char *body, size_t size) {
	uint32_t body_size = this->size() - LOG_EVENT_HEAD_LEN;
	if (size != body_size) {
		log_error("invalid log event body.");
		return -1;
	}

	if (body_size > 0) {
		buf.append(body, size);
	}

	return 0;
}

int LogEvent::inner_load() {
	assert (buf.size() >= LOG_EVENT_HEAD_LEN);

	// head only
	if (buf.size() == LOG_EVENT_HEAD_LEN)
		return 0;

	const char *begin = buf.data() + LOG_EVENT_HEAD_LEN;
	const char *end = buf.data() + size();

	// key
	CHECK_OUT_OF_SIZE(begin + sizeof(uint32_t));
	uint32_t key_len = unpack32(begin);
	begin += sizeof(uint32_t);
	CHECK_OUT_OF_SIZE(begin + key_len);
	this->_key = Bytes(begin, key_len);
	begin += key_len;

	if (begin == end) {
		return 0;
	}

	// val
	CHECK_OUT_OF_SIZE(begin + sizeof(uint32_t));
	uint32_t val_len = unpack32(begin);
	begin += sizeof(uint32_t);
	CHECK_OUT_OF_SIZE(begin + val_len);
	this->_val = Bytes(begin, val_len);
	begin += val_len;

	if (begin != end) {
		log_error("invalid log body");
		return -1;
	}

	return 0;
}

void LogEvent::pack32(std::string &s, uint32_t n) {
	uint32_t after = encode_uint32(n);
	s.append((char *)(&after), sizeof(uint32_t));
}

void LogEvent::pack64(std::string &s, uint64_t n) {
	uint64_t after = encode_uint64(n);
	s.append((char *)(&after), sizeof(uint64_t));
}

uint32_t LogEvent::unpack32(const char *buf) {
	return decode_uint32(*((uint32_t *)buf));
}

uint64_t LogEvent::unpack64(const char *buf) {
	return decode_uint64(*((uint64_t *)buf));
}
