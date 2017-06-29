/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#ifndef SSDB_LOG_READER_WRITER_H_
#define SSDB_LOG_READER_WRITER_H_

#include <string>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include "../util/io_cache.h"
#include "logevent.h"

class LogFile {
public:
	std::string filename;
	int fd;

public:
	LogFile() : fd(-1) {}
	LogFile(const char *name) : filename(name), fd(-1) {}
	~LogFile() {}

public:
	int creat(mode_t mode) {
		fd = ::creat(filename.c_str(), mode);
		if (fd < 0) {
			return -1;
		}
		return 0;
	}

	int open(int oflag, mode_t mode) {
		fd = ::open(filename.c_str(), oflag, mode);
		if (fd < 0) {
			return -1;
		}
		return 0;
	}

	int close() {
		if (fd > 0) {
			::close(fd);
			fd = -1;
		}
		return 0;
	}

	bool same_file(const LogFile *logfile) const {
		return this->filename == logfile->filename;
	}
};

class LogReader {
private:
	ReadCache *read_cache;
	LogFile *logfile;

public:
	LogReader(size_t cache_size=128*1024)
	: read_cache(new ReadCache(cache_size))
	, logfile(NULL) { }
	~LogReader() {
		if (read_cache) {
			delete read_cache;
		}
	}

public:
	void bind(LogFile *file);
	void unbind();

public:
	/*
	 * return 0 represents read EOF,
	 * 1 represents read logevent success and contained in event,
	 * less than 0 represents error.
	 */
	int read(LogEvent *event);

	/*
	 * seek exactly after the event has seq of @seq.
	 */
	int seek_to_seq(uint64_t seq);
};

class LogWriter {
private:
	WriteCache *write_cache;
	LogFile *logfile;

public:
	LogWriter(size_t cache_size)
	: write_cache(new WriteCache(cache_size))
	, logfile(NULL) { }
	~LogWriter() {
		if (write_cache) {
			delete write_cache;
		}
	}

public:
	void bind(LogFile *file);
	void unbind();

public:
	bool valid() const {
		return logfile != NULL;
	}

public:
	int write(LogEvent *event);
	int flush_to_file();
	int sync_file();
};

#endif
