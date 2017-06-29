/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_H_
#define SSDB_H_

#include <vector>
#include <string>
#include "const.h"
#include "options.h"
#include "iterator.h"
#include "leveldb/db.h"
#include "transaction.h"

class Bytes;
class Config;
class SlotBytewiseComparatorImpl;

class SSDB{
public:
	SSDB(){}
	virtual ~SSDB(){};
	static SSDB* open(const Options &opt, const std::string &base_dir);
	virtual int init(const std::string &name) = 0;

	virtual int flushdb() = 0;

	// return (start, end], not include start
	virtual Iterator* iterator(const std::string &start, const std::string &end, uint64_t limit, const leveldb::Snapshot *snapshot=NULL) = 0;
	virtual Iterator* rev_iterator(const std::string &start, const std::string &end, uint64_t limit, const leveldb::Snapshot *snapshot=NULL) = 0;

	//void flushdb();
	virtual uint64_t size() = 0;
	virtual std::vector<std::string> info() = 0;
	virtual void compact() = 0;
	virtual int key_range(std::vector<std::string> *keys) = 0;

	/* version control */
	virtual int new_version(const Bytes &key, char t, uint64_t *version) = 0;
	virtual int get_version(const Bytes &key, char *t, uint64_t *version, const leveldb::Snapshot *snapshot=NULL) = 0;

	/* raw operates */
	virtual int raw_set(const Bytes &key, const Bytes &val) = 0;
	virtual int raw_del(const Bytes &key) = 0;
	virtual int raw_get(const Bytes &key, std::string *val, const leveldb::Snapshot *snapshot=NULL) = 0;
	virtual int raw_size(const Bytes &key, int64_t *size) = 0;
	virtual int incr_raw_size(const Bytes &key, int64_t incr, int64_t *size, Transaction &trans) = 0;

	/* key value */
	virtual int set(const Bytes &key, const Bytes &val, Transaction &trans, uint64_t version) = 0;
	virtual int del(const Bytes &key, Transaction &trans) = 0;
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int incr(const Bytes &key, int64_t by, int64_t *new_val, Transaction &trans, uint64_t version) = 0;
	virtual int setbit(const Bytes &key, int bitoffset, int on, Transaction &trans, uint64_t version) = 0;
	virtual int getbit(const Bytes &key, int bitoffset, uint64_t version) = 0;

	virtual int get(const Bytes &key, std::string *val, uint64_t version) = 0;
	virtual int mget(const std::vector<Bytes> &key, std::vector<std::string> *val, std::vector<uint64_t> version, const leveldb::Snapshot *snapshot) = 0;
	// return (start, end]
	virtual KIterator* scan(const Bytes &start, const Bytes &end, uint64_t limit) = 0;
	virtual KIterator* rscan(const Bytes &start, const Bytes &end, uint64_t limit) = 0;

	/* hash */

	virtual int hset(const Bytes &key, const Bytes &field, const Bytes &val, Transaction &trans, uint64_t version) = 0;
	virtual int hdel(const Bytes &key, const Bytes &field, Transaction &trans, uint64_t version) = 0;
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int hincr(const Bytes &key, const Bytes &field, int64_t by, int64_t *new_val, Transaction &trans, uint64_t version) = 0;

	virtual int64_t hsize(const Bytes &key, uint64_t version) = 0;
	virtual int64_t hclear(const Bytes &key, Transaction &trans, uint64_t version) = 0;
	virtual int hget(const Bytes &key, const Bytes &field, std::string *val, uint64_t version) = 0;
	virtual int hlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list) = 0;
	virtual int hrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list) = 0;
	virtual HIterator* hscan(const Bytes &key, const Bytes &start, const Bytes &end, uint64_t limit, uint64_t version) = 0;
	virtual HIterator* hrscan(const Bytes &key, const Bytes &start, const Bytes &end, uint64_t limit, uint64_t version) = 0;

	/* zset */

	virtual int zset(const Bytes &key, const Bytes &field, const Bytes &score, Transaction &trans, uint64_t version) = 0;
	virtual int zdel(const Bytes &key, const Bytes &field, Transaction &trans, uint64_t version) = 0;
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int zincr(const Bytes &key, const Bytes &field, int64_t by, int64_t *new_val, Transaction &trans, uint64_t version) = 0;
	virtual int64_t zclear(const Bytes &key, Transaction &trans, uint64_t version) = 0;
	virtual int64_t zsize(const Bytes &key, uint64_t version) = 0;
	/**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(const Bytes &key, const Bytes &field, std::string *score, uint64_t version, const leveldb::Snapshot *snapshot=NULL) = 0;
	virtual int64_t zrank(const Bytes &key, const Bytes &field, uint64_t version) = 0;
	virtual int64_t zrrank(const Bytes &key, const Bytes &field, uint64_t version) = 0;
	virtual ZIterator* zrange(const Bytes &key, int64_t start, int64_t stop, uint64_t version) = 0;
	virtual ZIterator* zrrange(const Bytes &key, int64_t start, int64_t stop, uint64_t version) = 0;
	virtual ZIterator* zrange(const Bytes &key, uint64_t offset, uint64_t limit, uint64_t version) = 0;
	virtual ZIterator* zrrange(const Bytes &key, uint64_t offset, uint64_t limit, uint64_t version) = 0;
	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
	virtual ZIterator* zscan(const Bytes &key, const Bytes &field,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit, uint64_t version) = 0;
	virtual ZIterator* zrscan(const Bytes &key, const Bytes &field,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit, uint64_t version) = 0;
	virtual int zlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list) = 0;
	virtual int zrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list) = 0;
	/* set */
	virtual int sget(const Bytes &key, const Bytes &elem, uint64_t version) = 0;
	virtual int sset(const Bytes &key, const Bytes &elem, Transaction &trans, uint64_t version) = 0;
	virtual int sdel(const Bytes &key, const Bytes &elem, Transaction &trans, uint64_t version) = 0;
	virtual int64_t sclear(const Bytes &key, Transaction &trans, uint64_t version) = 0;
	virtual int64_t ssize(const Bytes &key, uint64_t version) = 0;
	virtual SIterator *sscan(const Bytes &key, const Bytes &elem, uint64_t limit, uint64_t version) = 0;
	virtual SIterator *srscan(const Bytes &key, const Bytes &elem, uint64_t limit, uint64_t version) = 0;

	virtual int64_t qsize(const Bytes &key, uint64_t version) = 0;
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int qfront(const Bytes &key, std::string *item, uint64_t version) = 0;
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int qback(const Bytes &key, std::string *item, uint64_t version) = 0;
	// @return -1: error, other: the new length of the queue
	virtual int64_t qpush_front(const Bytes &key, const Bytes &item, Transaction &trans, uint64_t version) = 0;
	virtual int64_t qpush_back(const Bytes &key, const Bytes &item, Transaction &trans, uint64_t version) = 0;
	// @return 0: empty queue, 1: item popped, -1: error
	virtual int qpop_front(const Bytes &key, std::string *item, Transaction &trans, uint64_t version) = 0;
	virtual int qpop_back(const Bytes &key, std::string *item, Transaction &trans, uint64_t version) = 0;
	virtual int qfix(const Bytes &name, Transaction &trans) = 0;
	virtual int qlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list) = 0;
	virtual int qrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list) = 0;
	virtual int qslice(const Bytes &name, int64_t offset, int64_t limit, uint64_t version,
			std::vector<std::string> *list) = 0;
	virtual int qget(const Bytes &key, int64_t index, std::string *item, uint64_t version) = 0;
	virtual int qset(const Bytes &key, int64_t index, const Bytes &item, Transaction &trans, uint64_t version) = 0;
	virtual int64_t qclear(const Bytes &key, Transaction &trans, uint64_t version) = 0;
	virtual QIterator *qscan(const Bytes &key, uint64_t seq_start, uint64_t limit, uint64_t version) = 0;

	virtual const leveldb::Snapshot *get_snapshot() = 0;
	virtual void release_snapshot(const leveldb::Snapshot *snapshot) = 0;

	// concurrent control
	virtual void lock_key(const std::string &key) = 0;
	virtual void unlock_key(const std::string &key) = 0;
	virtual void lock_db() = 0;
	virtual void unlock_db() = 0;
	virtual Iterator* keys(int16_t slot) = 0;

	//
	virtual leveldb::Status write(const leveldb::WriteOptions &options, leveldb::WriteBatch *batch) = 0;
};

#endif
