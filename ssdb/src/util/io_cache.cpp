/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include "io_cache.h"


int WriteCache::append(const char *data, int size) {
	// flush cache to file
	if (space() < size && flush_to_file() != 0) {
		return -1;
	}

	// write file directly
	if (size > cap) {
		int writebytes = writen(fd, data, size);
		if (writebytes != size)
			return -1;
		return 0;
	}

	// append to cache
	memcpy(end, data, size);
	end += size;
	return 0;
}

int WriteCache::append(const std::string &s) {
	return append(s.data(), s.size());
}

int WriteCache::flush_to_file() {
	int len = data_len();
	int ret = writen(fd, buf, len);
	if (ret != len)
		return -1;

	reset();
	return 0;
}

int WriteCache::sync_file() {
	int ret = fsync(fd);
	return ret;
}

int WriteCache::writen(int fd, const char *buf, size_t n) {
	int left = n;
	int err = 0;
	while (left > 0) {
		int ret = write(fd, buf+n-left, left);
		if (ret < 0 && (err = errno) == EINTR)
			continue;
		if (ret < 0) {
			return ret;
		}
		left -= ret;
	}

	return (int)(n - left);
}

int ReadCache::readn(char *dst, int n) {
	assert (n > 0);

	if (this->left() >= n) {
		memcpy(dst, cur, n);
		cur += n;
		return n;
	}

	int left = n;
	if (this->left() > 0) {
		memcpy(dst, cur, this->left());
		left -= this->left();
	}

	offset += (uint64_t)(end-buf);
	reset();

	int ret = 0;
	if (left > cap / 2) { // decrease data copy
		ret = readn(fd, dst+n-left, left);
		if (ret < 0) {
			return ret;
		}
		left -= ret;
		offset += ret;
	} else {
		ret = readn(fd, buf, cap);
		if (ret < 0) {
			return ret;
		}
		if (ret > 0) {
			cur = buf;
			end = cur + ret;
			int bytes2read = left < this->left() ? left : this->left();
			memcpy(dst+n-left, cur, bytes2read);
			cur += bytes2read;
			left -= bytes2read;
		}
	}

	return n - left;
}

int ReadCache::readn(int fd, char *buf, int n) {
	int err = 0;
	int left = n;
	while (left > 0) {
		int ret = read(fd, buf+n-left, left);
		if (ret < 0 && (err=errno) == EINTR)
			continue;
		if (ret < 0) {
			return -1;
		}
		if (ret == 0) //EOF
			break;
		left -= ret;
	}

	return n - left;
}

int ReadCache::seek(uint64_t pos) {
	if (pos < offset) {
		return -1;
	}

	while (offset+(uint64_t)(end-buf) < pos) {
		offset += (uint64_t)(end-buf);
		reset();

		int ret = readn(fd, buf, cap);
		if (ret < 0) {
			return -1;
		}
		if (ret == 0) break;
		if (ret > 0) {
			end += ret;
		}
	}

	if (offset+(uint64_t)(end-buf) < pos) {
		return -1;
	}

	cur += (pos - offset) - (cur - buf);
	assert (cur <= end);
	return 0;
}

