/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_IO_CACHE_H_
#define UTIL_IO_CACHE_H_

#include <string>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <stdint.h>

struct WriteCache {
public:
	char *buf;
	char *end;
	size_t cap;

	int fd;

public:
	WriteCache(size_t size) {
		assert (cap > 0);
		buf = (char *)malloc(size);
		assert (buf);
		end = buf;
		cap = size;
		fd = -1;
	}
	~WriteCache() {
		if (buf) {
			free(buf);
		}
	}

public:
	size_t data_len() { return (size_t)(end - buf); }
	size_t space() { return cap - data_len(); }

	void reset() { end = buf; }

	int append(const char *data, int size);
	int append(const std::string &s);

	int flush_to_file();
	int sync_file();

private:
	static int writen(int fd, const char *buf, size_t n);
};

struct ReadCache {
public:
	char *buf;
	char *cur;
	char *end;
	size_t cap;

	int fd;

	// buffer offset to file
	uint64_t offset;

public:
	ReadCache(size_t size) {
		assert (size > 0);
		buf = (char *)malloc(size);
		cur = buf;
		end = buf;
		cap = size;
		fd = -1;
		offset = 0;
	}
	~ReadCache() {
		if (buf) {
			free(buf);
		}
	}

	void bindfile(int fd) {
		reset();
		this->fd = fd;

		if (fd > 0) {
			lseek(fd, 0, SEEK_SET);
			offset = 0;
		}
	}

public:
	int readn(char *dst, int n);
	void reset() { cur = buf; end = cur; }
	size_t left() { return (size_t)(end - cur); }

public:
	uint64_t tell() const {
		return offset + (uint64_t)(cur - buf);
	}
	int seek(uint64_t off);

private:
	static int readn(int fd, char *buf, int n);
};

#endif
