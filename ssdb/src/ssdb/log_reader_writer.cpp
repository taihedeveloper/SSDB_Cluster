/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "log_reader_writer.h"
#include "const.h"
#include "../include.h"
#include "../util/log.h"
#include "../util/strings.h"
#include <map>
#include <stdlib.h>


/* LogReader */

void LogReader::bind(LogFile *file) {
	assert (logfile == NULL && file != NULL);

	logfile = file;
	read_cache->bindfile(logfile->fd);
}

void LogReader::unbind() {
	logfile = NULL;
	read_cache->fd = -1;
	read_cache->offset = 0;
	read_cache->reset();
}

#define LOGREADER_STAGE_HEAD 0
#define LOGREADER_STAGE_BODY 1

#define LOGREADER_RETRY_MAX 2
#define LOGREADER_SLEEP_SPAN 20000

int LogReader::read(LogEvent *event) {
	assert(event != NULL);
	event->clear();

	char head[LOG_EVENT_HEAD_LEN];
	char *buf = head;
	int retry = 0;
	int stage = LOGREADER_STAGE_HEAD;
	uint32_t total_size = LOG_EVENT_HEAD_LEN;
	uint32_t nread = 0;

	while (true) {
		/* read data */
		int ret = read_cache->readn(buf+nread, total_size-nread);
		if (ret < 0) {
			/* read file error */
			return -1;
		}
		if (ret == 0) {
			if (stage == LOGREADER_STAGE_HEAD && nread == 0) {
				/* no data to read */
				return 0;
			}
			if (retry == LOGREADER_RETRY_MAX) {
				/* EOF, prossible a bad section needed to ignore */
				return 0;
			}
			retry++;
		} else {
			/* reset retry */
			retry = 0;
		}

		nread += ret;
		if (nread < total_size) {
			/* sleep for a while */
			log_info("read pause for awhile, stage %d retry %d/%d nread %lu expect %lu", stage, retry, LOGREADER_RETRY_MAX, nread, total_size);
			usleep(LOGREADER_SLEEP_SPAN);
			continue;
		}
		if (nread > total_size) {
			return -1;
		}

		if (stage == LOGREADER_STAGE_HEAD) {
			/* prepare to read body */
			total_size = LogEvent::unpack32(head);
			event->repr().resize(total_size);
			buf = (char*)event->repr().data();
			memcpy((void*)buf, (void*)head, LOG_EVENT_HEAD_LEN);

			if (total_size - LOG_EVENT_HEAD_LEN == 0) {
				/* OK, head only */
				return 1;
			}
			stage = LOGREADER_STAGE_BODY;
		} else {
			if (event->inner_load() != 0) {
				log_error("inner load failed");
				return -1;
			}

			/* prase done */
			return 1;
		}
	}
}

int LogReader::seek_to_seq(uint64_t seq) {
	assert (read_cache->fd > 0);

	char header[LOG_EVENT_HEAD_LEN];
	while (1) {
		int ret = read_cache->readn(header, LOG_EVENT_HEAD_LEN);
		if (ret == 0) { // EOF
			log_error("seq (%lu) not in this file.", seq);
			return -1;
		}
		if (ret != LOG_EVENT_HEAD_LEN) {
			log_error("seek_to_seq read event header failed.");
			return -1;
		}

		uint64_t event_seq = LogEvent::unpack64(header+sizeof(uint32_t));
		if (event_seq <= seq) {
			uint64_t next_event_pos = read_cache->tell() + LogEvent::unpack32(header) - LOG_EVENT_HEAD_LEN;
			if (read_cache->seek(next_event_pos) != 0) {
				log_error("seek_to_seq read cache seek failed.");
				return -1;
			}
		}
		if (event_seq == seq) {
			break;
		}
		if (event_seq > seq) {
			log_error("seq out of order, no seq(%lu).", seq);
			return -1;
		}
	}

	return 0;
}

/* LogWriter */

void LogWriter::bind(LogFile *file) {
	assert (logfile == NULL && file != NULL);

	this->logfile = file;
	this->write_cache->reset();
	this->write_cache->fd = logfile->fd;
}

void LogWriter::unbind() {
	if (logfile) {
		flush_to_file();
		logfile = NULL;
	}
}

int LogWriter::write(LogEvent *event) {
	int ret = write_cache->append(event->repr());
	if (ret != 0) {
		log_error("write logevent failed");
	}
	return ret;
}

int LogWriter::flush_to_file() {
	return write_cache->flush_to_file();
}

int LogWriter::sync_file() {
	return write_cache->sync_file();
}

