/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "binlog2.h"
#include "const.h"
#include "../include.h"
#include "../util/log.h"
#include "../util/strings.h"
#include "../util/file.h"
#include "../util/spin_lock.h"

#include <map>
#include <stdlib.h>

#define BINLOG_READ_BUFFER_SIZE		1024*1024
#define BINLOG_WRITE_BUFFER_SIZE	1024*1024

const std::string SSDB_BinLog::BINLOG_FILE_LIST = "\xff\xff\xff\xff\xff|BINLOG_FILE_LIST|QUEUE";
const std::string SSDB_BinLog::BINLOG_LAST_SEQ = "\xff\xff\xff\xff\xff|BINLOG_LAST_SEQ|KV";

SSDB_BinLog::SSDB_BinLog(SSDB *meta, const std::string &dir, uint64_t max_binlog_size, bool sync, time_t purge_span) {
	this->meta = meta;
	if (dir.empty()) {
		this->binlog_dir = "./";
	} else {
		this->binlog_dir = dir;
	}
	this->next_file_num = -1;
	this->last_seq = 0;
	this->sync_binlog = sync;

	this->active_log = new LogFile();
	this->writer = new LogWriter(BINLOG_WRITE_BUFFER_SIZE);

	this->max_binlog_size = max_binlog_size;
	this->bytes_written = 0;
	this->active_log_size = 0;

	this->purge_logs_span = purge_span;

	pthread_mutex_init(&this->mutex, NULL);
	pthread_cond_init(&this->cond, NULL);
}

SSDB_BinLog::~SSDB_BinLog() {
	this->stop();

	if (active_log) {
		delete active_log;
	}
	if (writer) {
		delete writer;
	}
}

int SSDB_BinLog::erase_befores(size_t idx) {
	Transaction trans(this->meta, BINLOG_FILE_LIST);
	while (idx-- > 0) {
		std::string filename;
		if (this->meta->qpop_front(BINLOG_FILE_LIST, &filename, trans, 0) < 0) {
			log_error("erase_befores qpop_front failed");
			return -1;
		}
		log_info("erase_befores pop binlog(%s).", filename.c_str());
	}

	return 0;
}

int SSDB_BinLog::extract_file_num(const std::string &filename) {
	// like "00000001.binlog"
	size_t idx = filename.find_first_of('.');
	if (idx == std::string::npos) { return -1; }

	std::string number(filename.data(), idx);
	return str_to_int(number);
}

#define MARK_BINLOGS(x) \
do { \
	last_file_num = -1; \
	tmp_last_seq = 0; \
	first_valid_binlog_idx = x; \
	files.clear(); \
	files_min_seq.clear(); \
} while (0)

int SSDB_BinLog::save_last_seq() {
	int ret = 0;
	Transaction trans(this->meta, BINLOG_LAST_SEQ);
	ret = this->meta->set(BINLOG_LAST_SEQ, str(last_seq), trans, 0);
	if (ret < 0) {
		log_error("save binlog last seq failed.");
	}
	return ret;
}

int SSDB_BinLog::load_last_seq() {
	int ret = 0;
	std::string val;
	ret = this->meta->get(BINLOG_LAST_SEQ, &val, 0);
	if (ret < 0) {
		log_error("load binlog last seq failed.");
		return -1;
	} else if (ret == 1) {
		last_seq = str_to_uint64(val);
		return 1;
	} else if (ret == 0) {
		last_seq = 0;
		save_last_seq();
		return 0;
	}

	return 0;
}

std::string SSDB_BinLog::generate_name(int file_num) {
	char buf[255];
	int len = snprintf(buf, 255, "%08d.binlog", file_num);
	return std::string(buf, len);
}

int SSDB_BinLog::recover() {
	assert (this->meta);

	// load binlog last seq
	int ret = load_last_seq();
	if (ret < 0) {
		log_error("load last seq failed.");
		return -1;
	}

	// load binlog name list
	std::vector<std::string> tmp_files;
	ret = this->meta->qslice(BINLOG_FILE_LIST, 0, -1, 0, &tmp_files);
	if (ret < 0) {
		log_error("get binlog file list from meta failed");
		return ret;
	}

	// extract min seq for each binlog
	int last_file_num = -1;
	if (!tmp_files.empty()) {
		LogReader reader(16*1024);
		LogEvent event;
		uint64_t tmp_last_seq = 0;
		size_t first_valid_binlog_idx = 0;
		for (size_t i = 0; i < tmp_files.size(); i++) {
			std::string filename = binlog_dir + "/" + tmp_files[i];
			if (!file_exists(filename)) {
				log_warn("binlog (%s) not exist", filename.c_str());
				MARK_BINLOGS(i+1);
				continue;
			}

			int file_num = extract_file_num(tmp_files[i]);
			if (file_num < 0) {
				log_warn("invalid binlog filename(%s)", filename.c_str());
				MARK_BINLOGS(i+1);
				continue;
			}
			log_info("binlog recover last_file_num: %d  file_num: %d, filename: %s", last_file_num, file_num, tmp_files[i].c_str());
			if (last_file_num < 0) {
				last_file_num = file_num;
			} else if (last_file_num != file_num - 1) {
				log_warn("binlog filename out of order");
				MARK_BINLOGS(i+1);
				continue;
			}
			last_file_num = file_num;
			files.push_back(tmp_files[i]);

			LogFile logfile(filename.c_str());
			if (logfile.open(O_RDONLY, 0644) != 0) {
				log_error("open binlog (%s) failed", filename.c_str());
				return -1;
			}

			// read binlog first desc event
			reader.bind(&logfile);
			if (reader.read(&event) != 1) {
				log_error("read first desc event from (%s) faild", filename.c_str());
				MARK_BINLOGS(i+1);
				continue;
			}
			if (event.seq() < tmp_last_seq || (event.seq() == tmp_last_seq && event.cmd() != BinlogCommand::DESC)) {
				log_error("seq out of order");
				MARK_BINLOGS(i+1);
				continue;
			}
			tmp_last_seq = event.seq();
			files_min_seq.insert(std::make_pair<std::string, uint64_t>(tmp_files[i], tmp_last_seq));

			reader.unbind();
			logfile.close();
		}

		if (first_valid_binlog_idx > 0) {
			this->erase_befores(first_valid_binlog_idx);
		}
	}

	// new active binlog
	this->next_file_num = last_file_num > 0 ? last_file_num+1 : 1;
	if (new_file() < 0) {
		log_error("new binlog file failed.");
		return -1;
	}

	return 0;
}

int SSDB_BinLog::rotate() {
	if (!is_open()) return 0;

	std::string next_filename = generate_name(next_file_num);

	/* don't increase seq for rotate event */
	LogEvent rotate_event(last_seq, BinlogType::SYNC, BinlogCommand::ROTATE,
			Bytes(next_filename));
	if (writer->write(&rotate_event) != 0) {
		log_error("write rotate event failed.");
		goto err;
	}

	if (new_file() != 0) {
		log_error("create new binlog file failed.");
		goto err;
	}

	/* purge logs */
	if (purge_logs_span > 0) {
		if (purge_logs_before_date(time(NULL) - purge_logs_span) != 0) {
			log_warn("purge logs failed");
		}
	}

	return 0;

err:
	log_error("rotate binlog failed.");
	return -1;
}

int SSDB_BinLog::incr_seq() {
	last_seq++;

	if (save_last_seq() < 0) {
		log_error("save last seq failed.");
		return -1;
	}

	return 0;
}

int SSDB_BinLog::new_file() {
	assert (next_file_num > 0);

	// hold the lock
	WriteLockGuard<RWLock> guard(this->rwlock);

	// flush data to file
	writer->unbind();
	active_log->close();

	// create new binlog file
	std::string filename = generate_name(next_file_num);
	active_log->filename = binlog_dir + "/" + filename;
	unlink(active_log->filename.c_str());
	int ret = active_log->creat(0644);
	if (ret < 0) {
		log_error("create new binlog file(%s) failed, errno(%d).",
				active_log->filename.c_str(), errno);
	}
	writer->bind(active_log);

	/* don't increase seq for new file */
	LogEvent desc_event(last_seq, BinlogType::SYNC, BinlogCommand::DESC);
	writer->write(&desc_event);
	writer->flush_to_file();

	// save new binlog filename
	next_file_num++;
	Transaction trans(this->meta, BINLOG_FILE_LIST);
	if (this->meta->qpush_back(BINLOG_FILE_LIST, filename, trans, 0) < 0) {
		log_error("save new binlog file failed");
		return -1;
	}
	files.push_back(filename);
	files_min_seq.insert(std::make_pair<std::string, uint64_t>(filename, last_seq));

	return 0;
}

int SSDB_BinLog::check_rotate() {
	if (max_binlog_size > 0 && active_log_size > max_binlog_size) {
		active_log_size = 0;
		if (rotate() != 0) {
			log_error("rotate binlog failed.");
			return -1;
		}
	}

	return 0;
}

int SSDB_BinLog::write_impl(LogEvent *event) {
	if (!is_open()) {
		return 0;
	}

	if (check_rotate() != 0) {
		return -1;
	}

	if (event) {
		statistic(event->repr().size());
		return writer->write(event);
	}

	return 0;
}

int SSDB_BinLog::write_impl(LogEventBatch *batch) {
	if (!is_open() || !batch) {
		return 0;
	}

	if (check_rotate() != 0) {
		return -1;
	}

	for (size_t i = 0; i < batch->events.size(); i++) {
		if (batch->events[i]) {
			statistic(batch->events[i]->repr().size());
			if (writer->write(batch->events[i]) != 0) {
				return -1;
			}
		}
	}

	return 0;
}

int SSDB_BinLog::write(LogEvent *event) {
	int ret = 0;
	ret = this->write_impl(event);
	if (ret != 0) {
		log_error("write event failed. ret(%d).", ret);
		return ret;
	}

	ret = this->flush();
	if (ret != 0) {
		log_error("flush binlog failed. ret(%d).", ret);
		return ret;
	}

	if (sync_binlog) {
		ret = this->sync();
		if (ret != 0) {
			log_error("sync binlog failed. ret(%d).", ret);
			return ret;
		}
	}

	return ret;
}

int SSDB_BinLog::write(char type, char cmd) {
	int ret = 0;

	this->pre_write();

	uint64_t target_seq = SSDB_BINLOG_RESEVE_SEQ;
	unsigned char v = (unsigned char)cmd;
	if (v >= SSDB_SYNC_CMD_MIN && v <= SSDB_SYNC_CMD_MAX) {
		/* incr seq for validate cmd only */
		this->incr_seq();
		target_seq = last_seq;
	}

	LogEvent event(target_seq, type, cmd);
	ret = write(&event);

	this->post_write();

	return ret;
}

int SSDB_BinLog::write(char type, char cmd, const Bytes &key, uint64_t ttl) {
	int ret = 0;

	this->pre_write();

	uint64_t target_seq = SSDB_BINLOG_RESEVE_SEQ;
	unsigned char v = (unsigned char)cmd;
	if (v >= SSDB_SYNC_CMD_MIN && v <= SSDB_SYNC_CMD_MAX) {
		/* incr seq for validate cmd only */
		this->incr_seq();
		target_seq = last_seq;
	}

	LogEvent event(target_seq, type, cmd, key, ttl);
	ret = write(&event);

	this->post_write();

	return ret;
}

int SSDB_BinLog::write(char type, char cmd, const Bytes &key, const Bytes &val, uint64_t ttl) {
	int ret = 0;

	this->pre_write();

	uint64_t target_seq = SSDB_BINLOG_RESEVE_SEQ;
	unsigned char v = (unsigned char)cmd;
	if (v >= SSDB_SYNC_CMD_MIN && v <= SSDB_SYNC_CMD_MAX) {
		/* incr seq for validate cmd only */
		this->incr_seq();
		target_seq = last_seq;
	}
	LogEvent event(target_seq, type, cmd, key, val, ttl);
	ret = write(&event);

	this->post_write();

	return ret;
}

int SSDB_BinLog::flush() {
	return writer->flush_to_file();
}

int SSDB_BinLog::sync() {
	return writer->sync_file();
}

std::string SSDB_BinLog::find_binlog(uint64_t seq) const {
	ReadLockGuard<RWLock> guard(this->rwlock);

	std::string last_binlog;
	std::map<std::string, uint64_t>::const_iterator it;
	for (it = files_min_seq.begin(); it != files_min_seq.end(); it++) {
		if (it->second < seq) {
			last_binlog = it->first;
		} else if (it->second == seq) {
			return it->first;
		} else if (it->second > seq)
			break;
	}

	return last_binlog;
}

int SSDB_BinLog::stop() {
	if (!is_open()) return 0;

	/* don't increase seq for stop event */
	LogEvent stop_event(last_seq, BinlogType::SYNC, BinlogCommand::STOP);
	if (writer->write(&stop_event) != 0 || writer->flush_to_file() != 0) {
		log_error("write stop event failed.");
		return -1;
	}

	active_log->close();

	return 0;
}

int SSDB_BinLog::clean() {
	std::string logname;
	Transaction trans(this->meta, BINLOG_FILE_LIST);
	return this->meta->qclear(BINLOG_FILE_LIST, trans, 0);
}

int SSDB_BinLog::reset() {
	stop();
	last_seq = 0;
	save_last_seq();

	if (clean() < 0) {
		log_error("clean binlog failed");
		return -1;
	}

	this->next_file_num = 1;

	return new_file();
}

void SSDB_BinLog::incr_inuse(const std::string &binlog) {
	assert (!binlog.empty());

	WriteLockGuard<RWLock> guard(this->rwlock);

	std::map<std::string, uint32_t>::iterator it = inuse_binlogs.find(binlog);
	if (it == inuse_binlogs.end()) {
		inuse_binlogs.insert(std::make_pair<std::string, uint32_t>(binlog, 1));
	} else {
		it->second++;
	}
}

void SSDB_BinLog::decr_inuse(const std::string &binlog) {
	if (binlog.empty()) return;

	WriteLockGuard<RWLock> guard(this->rwlock);

	std::map<std::string, uint32_t>::iterator it = inuse_binlogs.find(binlog);
	if (it != inuse_binlogs.end()) {
		it->second--;
		if (it->second == 0) {
			inuse_binlogs.erase(binlog);
		}
	}
}

bool SSDB_BinLog::inuse(const std::string &binlog) {
	// assert this->rwlock held.

	assert (!binlog.empty());

	if (active_log && active_log->filename == binlog) {
		return true;
	}

	std::map<std::string, uint32_t>::iterator it = inuse_binlogs.find(binlog);
	if (it != inuse_binlogs.end()) {
		return true;
	}

	return false;
}

int SSDB_BinLog::purge_logs(const std::string &to_log, bool included) {
	WriteLockGuard<RWLock> guard(this->rwlock);

	// collect
	bool found = false;
	std::vector<std::string> files_to_purge;
	std::list<std::string>::const_iterator it = files.begin();
	for (; it != files.end(); it++) {
		if (*it == to_log) {
			found = true;
			if (included) {
				files_to_purge.push_back(*it);
			}

			break;
		}
		files_to_purge.push_back(*it);
	}

	if (!found) {
		log_error("invalid binlog name(%s).", to_log.c_str());
		return -2;
	}

	// check inuse
	size_t idx;
	for (idx = 0; idx < files_to_purge.size(); idx++) {
		if (inuse(files_to_purge[idx]))
			break;
	}

	// purge
	for (size_t i = 0; i < idx; i++) {
		std::string logname;
		int ret = this->meta->qfront(BINLOG_FILE_LIST, &logname, 0);
		if (ret != 1) {
			log_error("get binlog name failed.");
			return -1;
		}

		if (logname != files_to_purge[i]) {
			log_error("binlog index out of order");
			return -1;
		}

		{
			Transaction trans(this->meta, BINLOG_FILE_LIST);
			if (this->meta->qpop_front(BINLOG_FILE_LIST, &logname, trans, 0) != 1) {
				log_error("pop front binlog failed.");
				return -1;
			}
		}

		std::string filename = binlog_dir + "/" + logname;
		if (unlink(filename.c_str()) != 0) {
			log_error("remove binlog (%s) failed, errno(%d)", logname.c_str(), errno);
			return -1;
		}

		files.pop_front();
		files_min_seq.erase(logname);
	}

	return 0;
}

int SSDB_BinLog::purge_logs_before_date(time_t date) {

	std::string to_log;
	{
		WriteLockGuard<RWLock> guard(this->rwlock);

		std::list<std::string>::const_iterator it = files.begin();
		for (; it != files.end(); it++) {
			std::string filename = binlog_dir + "/" + (*it);
			time_t mtime = last_modify_time(filename);
			if (mtime > 0 && mtime < date) {
				to_log = *it;
			} else {
				break;
			}
		}
	}

	if (!to_log.empty()) {
		return purge_logs(to_log);
	}

	return 0;
}

std::string SSDB_BinLog::find_next(const std::string &curr) {
	ReadLockGuard<RWLock> guard(this->rwlock);

	std::string nextfile = "";
	std::map<std::string, uint64_t>::const_iterator it = files_min_seq.find(curr);
	if (it != files_min_seq.end() && ++it != files_min_seq.end()) {
		nextfile = it->first;
	}

	return nextfile;
}

bool SSDB_BinLog::is_active_log(const std::string &filename) const {
	ReadLockGuard<RWLock> guard(this->rwlock);

	if (active_log && active_log->filename == filename) {
		return true;
	}

	return false;
}
