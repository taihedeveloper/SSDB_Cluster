/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_IMPL_H_
#define SSDB_IMPL_H_

#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "../util/log.h"
#include "../util/config.h"

#include "ssdb.h"
#include "iterator.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_queue.h"
#include "concurrent.h"
#include "t_set.h"

inline
static leveldb::Slice slice(const Bytes &b){
	return leveldb::Slice(b.data(), b.size());
}

class SSDBImpl : public SSDB
{
private:
	friend class SSDB;
	leveldb::DB* ldb;
	leveldb::Options options;
	uint64_t global_version;             /* global version increase by 1 every time coming up to an delete */
	uint64_t version_update_threshold;   /* threshold to record global version */
	uint64_t num_version_update;         /* numbers of version updates since last record */
	int inited;
	std::string name;
	std::string dir;

	DBKeyLock *dblocks;

	SSDBImpl(int32_t concurrency=1024);

public:
	// concurrent control
	std::string get_name();
	virtual void lock_key(const std::string &key);
	virtual void unlock_key(const std::string &key);
	virtual void lock_db();
	virtual void unlock_db();
	virtual Iterator* keys(int16_t slot);

public:
	virtual ~SSDBImpl();

	virtual int init(const std::string &name);

	virtual int flushdb();

	virtual leveldb::Options get_options();

	// return (start, end], not include start
	virtual Iterator* iterator(const std::string &start, const std::string &end, uint64_t limit, const leveldb::Snapshot *snapshot=NULL);
	virtual Iterator* rev_iterator(const std::string &start, const std::string &end, uint64_t limit, const leveldb::Snapshot *snapshot=NULL);

	//void flushdb();
	virtual uint64_t size();
	virtual uint64_t leveldbfilesize();
	virtual std::vector<std::string> info();
	virtual void compact();
	virtual int key_range(std::vector<std::string> *keys);

	/* version control */
	virtual int new_version(const Bytes &key, char t, uint64_t *version);
	virtual int get_version(const Bytes &key, char *t, uint64_t *version, const leveldb::Snapshot *snapshot=NULL);

	/* raw operates */
	virtual int raw_set(const Bytes &key, const Bytes &val);
	virtual int raw_del(const Bytes &key);
	virtual int raw_get(const Bytes &key, std::string *val, const leveldb::Snapshot *snapshot=NULL);
	virtual int raw_size(const Bytes &key, int64_t *size);
	virtual int incr_raw_size(const Bytes &key, int64_t incr, int64_t *size, Transaction &trans);

	/* key value */
	virtual int set(const Bytes &key, const Bytes &val, Transaction &trans, uint64_t version);
	virtual int del(const Bytes &key, Transaction &trans);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int incr(const Bytes &key, int64_t by, int64_t *new_val, Transaction &trans, uint64_t version);
	virtual int setbit(const Bytes &key, int bitoffset, int on, Transaction &trans, uint64_t version);
	virtual int getbit(const Bytes &key, int bitoffset, uint64_t version);

	virtual int get(const Bytes &key, std::string *val, uint64_t version);
	virtual int mget(const std::vector<Bytes> &key, std::vector<std::string> *val, std::vector<uint64_t> version, const leveldb::Snapshot *snapshot);
	// return (start, end]
	virtual KIterator* scan(const Bytes &start, const Bytes &end, uint64_t limit);
	virtual KIterator* rscan(const Bytes &start, const Bytes &end, uint64_t limit);

	/* hash */
	virtual int hset(const Bytes &key, const Bytes &field, const Bytes &val, Transaction &trans, uint64_t version);
	virtual int hdel(const Bytes &key, const Bytes &field, Transaction &trans, uint64_t version);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int hincr(const Bytes &key, const Bytes &field, int64_t by, int64_t *new_val, Transaction &trans, uint64_t version);
	//int multi_hset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0, char log_type=BinlogType::SYNC);
	//int multi_hdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0, char log_type=BinlogType::SYNC);

	virtual int64_t hsize(const Bytes &key, uint64_t version);
	virtual int64_t hclear(const Bytes &key, Transaction &trans, uint64_t version);
	virtual int hget(const Bytes &key, const Bytes &field, std::string *val, uint64_t version);
	virtual int hlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list);
	virtual int hrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list);
	virtual HIterator* hscan(const Bytes &key, const Bytes &start, const Bytes &end, uint64_t limit, uint64_t version);
	virtual HIterator* hrscan(const Bytes &key, const Bytes &start, const Bytes &end, uint64_t limit, uint64_t version);

	/* zset */
	virtual int zset(const Bytes &key, const Bytes &field, const Bytes &score, Transaction &trans, uint64_t version);
	virtual int zdel(const Bytes &key, const Bytes &field, Transaction &trans, uint64_t version);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int zincr(const Bytes &key, const Bytes &field, int64_t by, int64_t *new_val, Transaction &trans, uint64_t version);
	//int multi_zset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0, char log_type=BinlogType::SYNC);
	//int multi_zdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0, char log_type=BinlogType::SYNC);

	virtual int64_t zsize(const Bytes &key, uint64_t version);
	/**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(const Bytes &key, const Bytes &field, std::string *score, uint64_t version, const leveldb::Snapshot *snapshot=NULL);
	virtual int64_t zrank(const Bytes &key, const Bytes &field, uint64_t version);
	virtual int64_t zrrank(const Bytes &key, const Bytes &field, uint64_t version);
	virtual int64_t zclear(const Bytes &key, Transaction &trans, uint64_t version);
	virtual ZIterator* zrange(const Bytes &key, int64_t start, int64_t stop, uint64_t version);
	virtual ZIterator* zrrange(const Bytes &key, int64_t start, int64_t stop, uint64_t version);
	virtual ZIterator* zrange(const Bytes &key, uint64_t offset, uint64_t limit, uint64_t version);
	virtual ZIterator* zrrange(const Bytes &key, uint64_t offset, uint64_t limit, uint64_t version);
	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
	virtual ZIterator* zscan(const Bytes &key, const Bytes &field,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit, uint64_t version);
	virtual ZIterator* zrscan(const Bytes &key, const Bytes &field,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit, uint64_t version);
	virtual int zlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list);
	virtual int zrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list);
	/* set */
	virtual int sget(const Bytes &key, const Bytes &elem, uint64_t version);
	virtual int sset(const Bytes &key, const Bytes &elem, Transaction &trans, uint64_t version);
	virtual int sdel(const Bytes &key, const Bytes &elem, Transaction &trans, uint64_t version);
	virtual int64_t sclear(const Bytes &key, Transaction &trans, uint64_t version);
	virtual int64_t ssize(const Bytes &key, uint64_t version);
	virtual SIterator *sscan(const Bytes &key, const Bytes &elem, uint64_t limit, uint64_t version);
	virtual SIterator *srscan(const Bytes &key, const Bytes &elem, uint64_t limit, uint64_t version);
	virtual int64_t qsize(const Bytes &key, uint64_t version);
	virtual int64_t qclear(const Bytes &key, Transaction &trans, uint64_t version);
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int qfront(const Bytes &key, std::string *item, uint64_t version);
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int qback(const Bytes &key, std::string *item, uint64_t version);
	// @return -1: error, other: the new length of the queue
	virtual int64_t qpush_front(const Bytes &key, const Bytes &item, Transaction &trans, uint64_t version);
	virtual int64_t qpush_back(const Bytes &key, const Bytes &item, Transaction &trans, uint64_t version);
	// @return 0: empty queue, 1: item popped, -1: error
	virtual int qpop_front(const Bytes &key, std::string *item, Transaction &trans, uint64_t version);
	virtual int qpop_back(const Bytes &key, std::string *item, Transaction &trans, uint64_t version);
	virtual int qfix(const Bytes &key, Transaction &trans);
	virtual int qlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list);
	virtual int qrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list);
	virtual int qslice(const Bytes &key, int64_t offset, int64_t limit, uint64_t version,
			std::vector<std::string> *list);
	virtual int qget(const Bytes &key, int64_t index, std::string *item, uint64_t version);
	virtual int qset(const Bytes &key, int64_t index, const Bytes &item, Transaction &trans, uint64_t version);
	virtual QIterator *qscan(const Bytes &key, uint64_t seq_start, uint64_t limit, uint64_t version);

private:
	int64_t _qpush(const Bytes &key, const Bytes &item, uint64_t front_or_back_seq, Transaction &trans, uint64_t version);
	int _qpop(const Bytes &key, std::string *item, uint64_t front_or_back_seq, Transaction &trans, uint64_t version);
	int _update_global_version();

public:
	// snapshot
	virtual const leveldb::Snapshot *get_snapshot();
	virtual void release_snapshot(const leveldb::Snapshot *snapshot);

	virtual leveldb::Status write(const leveldb::WriteOptions &options, leveldb::WriteBatch *batch);
};

#define SSDB_CHECK_KEY_LEN(k)\
do {\
	if((k).size() > SSDB_KEY_LEN_MAX) {\
		log_error("key too long");\
		return -1;\
	}\
} while(0)\

#define SSDB_CHECK_KEY_TYPE(tx, ty)\
do {\
	if(tx != ty) {\
		log_error("confilic key type");\
		return -1;\
	}\
} while(0)

#endif
