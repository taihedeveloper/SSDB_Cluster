/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "ssdb_impl.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"

#include "iterator.h"
#include "comparator.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_queue.h"
#include "version.h"

static void *ssdb_gc_thread(void *arg);

SSDBImpl::SSDBImpl(int32_t concurrency)
	: ldb(NULL), global_version(0), version_update_threshold(10000), num_version_update(0), inited(0){
	dblocks = new DBKeyLock(concurrency);
}

SSDBImpl::~SSDBImpl(){
	if(ldb){
		delete ldb;
	}
	if(options.block_cache){
		delete options.block_cache;
	}
	if(options.filter_policy){
		delete options.filter_policy;
	}
	if (dblocks) {
		delete dblocks;
	}
}

SSDB* SSDB::open(const Options &opt, const std::string &dir){
	SSDBImpl *ssdb = new SSDBImpl();
	ssdb->options.create_if_missing = true;
	ssdb->options.max_open_files = opt.max_open_files;
	ssdb->options.filter_policy = leveldb::NewBloomFilterPolicy(10);
	ssdb->options.block_cache = leveldb::NewLRUCache(opt.cache_size * 1048576);
	ssdb->options.block_size = opt.block_size * 1024;
	ssdb->options.write_buffer_size = opt.write_buffer_size * 1024 * 1024;
	ssdb->options.compaction_speed = opt.compaction_speed;
	ssdb->options.comparator = SlotBytewiseComparatorImpl::getComparator();
	if(opt.compression == "yes"){
		ssdb->options.compression = leveldb::kSnappyCompression;
	}else{
		ssdb->options.compression = leveldb::kNoCompression;
	}

	leveldb::Status status;

	ssdb->dir = dir;

	status = leveldb::DB::Open(ssdb->options, dir, &ssdb->ldb);
	if(!status.ok()){
		log_error("open db failed: %s", status.ToString().c_str());
		goto err;
	}

	return ssdb;
err:
	if(ssdb){
		delete ssdb;
	}
	return NULL;
}

int SSDBImpl::init(const std::string &name) {
	if(inited == 1) {
		log_warn("ssdb is already initiated");
		return 0;
	}
	this->name = name;
	std::string vg;
	std::string key = global_version_key();
	int ret = this->raw_get(key, &vg);
	if (ret == 0) {
		/* initiate global version */
		int r = this->raw_set(key, "0");
		if (r != 1) {
			log_error("initiate global version failed");
			return -1;
		}
	} else {
		/* step foward over a distance to guarantee there are no conflicts after a reboot */
		global_version = str_to_uint64(vg);
		global_version += version_update_threshold*10;
		ret = this->raw_set(key, str(global_version));
		if (ret != 1) {
			log_error("setp foward global_version failed");
			return -1;
		}
	}

	/* start gc thread */
	log_info("start gc");
	pthread_t tid;
	int err = pthread_create(&tid, NULL, &ssdb_gc_thread, this);
	if(err != 0) {
		log_error("start gc thread failed: %s", strerror(err));
		return -1;
	}
	inited = 1;
	return 1;
}

leveldb::Options SSDBImpl::get_options() {
	return options;
}

int SSDBImpl::flushdb(){
	this->lock_db();

	delete ldb;

	leveldb::Status status = leveldb::DestroyDB(dir, options);
	if (!status.ok()) {
		log_error("destroy db %s failed: %s", dir.c_str(), status.ToString().c_str());
		this->unlock_db();
		return -1;
	}

	status = leveldb::DB::Open(options, dir, &ldb);
	if (!status.ok()) {
		log_error("open db %s failed: %s", dir.c_str(), status.ToString().c_str());
		this->unlock_db();
		return -1;
	}

	this->unlock_db();

	return 0;
}

Iterator* SSDBImpl::iterator(const std::string &start, const std::string &end, uint64_t limit, const leveldb::Snapshot *snapshot){
	leveldb::Iterator *it;
	leveldb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;
	if (snapshot) {
		iterate_options.snapshot = snapshot;
	}
	it = ldb->NewIterator(iterate_options);
	it->Seek(start);
	if(it->Valid() && it->key() == start){
		it->Next();
	}
	return new Iterator(it, end, limit);
}

Iterator* SSDBImpl::rev_iterator(const std::string &start, const std::string &end, uint64_t limit, const leveldb::Snapshot *snapshot){
	leveldb::Iterator *it;
	leveldb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;
	if (snapshot) {
		iterate_options.snapshot = snapshot;
	}
	it = ldb->NewIterator(iterate_options);
	it->Seek(start);
	if(!it->Valid()){
		it->SeekToLast();
	}else{
		it->Prev();
	}
	return new Iterator(it, end, limit, Iterator::BACKWARD);
}

/* raw operates */

int SSDBImpl::raw_set(const Bytes &key, const Bytes &val){
	leveldb::WriteOptions write_opts;
	leveldb::Status s = ldb->Put(write_opts, slice(key), slice(val));
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::raw_del(const Bytes &key){
	leveldb::WriteOptions write_opts;
	leveldb::Status s = ldb->Delete(write_opts, slice(key));
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::raw_get(const Bytes &key, std::string *val, const leveldb::Snapshot *snapshot){
	leveldb::ReadOptions opts;
	if(snapshot) {
		opts.snapshot = snapshot;
	}
	//opts.fill_cache = false;
	leveldb::Status s = ldb->Get(opts, slice(key), val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

uint64_t SSDBImpl::size(){
	std::string s = "A";
	std::string e(1, 'z' + 1);
	leveldb::Range ranges[1];
	ranges[0] = leveldb::Range(s, e);
	uint64_t sizes[1];
	ldb->GetApproximateSizes(ranges, 1, sizes);
	return sizes[0];
}

uint64_t SSDBImpl::leveldbfilesize(){
	uint64_t size;
	ldb->GetDbSize(&size);
	return size;
}

std::vector<std::string> SSDBImpl::info(){
	//  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
	//     where <N> is an ASCII representation of a level number (e.g. "0").
	//  "leveldb.stats" - returns a multi-line string that describes statistics
	//     about the internal operation of the DB.
	//  "leveldb.sstables" - returns a multi-line string that describes all
	//     of the sstables that make up the db contents.
	std::vector<std::string> info;
	std::vector<std::string> keys;
	/*
	for(int i=0; i<7; i++){
		char buf[128];
		snprintf(buf, sizeof(buf), "leveldb.num-files-at-level%d", i);
		keys.push_back(buf);
	}
	*/
	keys.push_back("leveldb.stats");
	//keys.push_back("leveldb.sstables");

	for(size_t i=0; i<keys.size(); i++){
		std::string key = keys[i];
		std::string val;
		if(ldb->GetProperty(key, &val)){
			info.push_back(key);
			info.push_back(val);
		}
	}

	return info;
}

void SSDBImpl::compact(){
	ldb->CompactRange(NULL, NULL);
}

int SSDBImpl::key_range(std::vector<std::string> *keys){
	int ret = 0;
	std::string kstart, kend;
	std::string hstart, hend;
	std::string zstart, zend;
	std::string qstart, qend;

	Iterator *it;

	it = this->iterator(encode_kv_key("", 0), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::KV){
			std::string n;
			uint64_t v;
			if(decode_kv_key(ks, &n, &v) == -1){
				ret = -1;
			}else{
				kstart = n;
			}
		}
	}
	delete it;

	it = this->rev_iterator(encode_kv_key("\xff", 0), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::KV){
			std::string n;
			uint64_t v;
			if(decode_kv_key(ks, &n, &v) == -1){
				ret = -1;
			}else{
				kend = n;
			}
		}
	}
	delete it;

	it = this->iterator(encode_hsize_key("", 0), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::HSIZE){
			std::string n;
			uint64_t v;
			if(decode_hsize_key(ks, &n, &v) == -1){
				ret = -1;
			}else{
				hstart = n;
			}
		}
	}
	delete it;

	it = this->rev_iterator(encode_hsize_key("\xff", 0), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::HSIZE){
			std::string n;
			uint64_t v;
			if(decode_hsize_key(ks, &n, &v) == -1){
				ret = -1;
			}else{
				hend = n;
			}
		}
	}
	delete it;

	it = this->iterator(encode_zsize_key("", 0), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::ZSIZE){
			std::string n;
			uint64_t v;
			if(decode_hsize_key(ks, &n, &v) == -1){
				ret = -1;
			}else{
				zstart = n;
			}
		}
	}
	delete it;

	it = this->rev_iterator(encode_zsize_key("\xff", 0), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::ZSIZE){
			std::string n;
			uint64_t v;
			if(decode_hsize_key(ks, &n, &v) == -1){
				ret = -1;
			}else{
				zend = n;
			}
		}
	}
	delete it;

	it = this->iterator(encode_qsize_key("", 0), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::QSIZE){
			std::string n;
			uint64_t v;
			if(decode_qsize_key(ks, &n, &v) == -1){
				ret = -1;
			}else{
				qstart = n;
			}
		}
	}
	delete it;

	it = this->rev_iterator(encode_qsize_key("\xff", 0), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::QSIZE){
			std::string n;
			uint64_t v;
			if(decode_qsize_key(ks, &n, &v) == -1){
				ret = -1;
			}else{
				qend = n;
			}
		}
	}
	delete it;

	keys->push_back(kstart);
	keys->push_back(kend);
	keys->push_back(hstart);
	keys->push_back(hend);
	keys->push_back(zstart);
	keys->push_back(zend);
	keys->push_back(qstart);
	keys->push_back(qend);

	return ret;
}


const leveldb::Snapshot *SSDBImpl::get_snapshot() {
	return this->ldb->GetSnapshot();
}

void SSDBImpl::release_snapshot(const leveldb::Snapshot *snapshot) {
	this->ldb->ReleaseSnapshot(snapshot);
}

void SSDBImpl::lock_key(const std::string &key) {
	this->dblocks->lock(key);
}

void SSDBImpl::unlock_key(const std::string &key) {
	this->dblocks->unlock(key);
}

void SSDBImpl::lock_db() {
	this->dblocks->lock_db();
}

void SSDBImpl::unlock_db() {
	this->dblocks->unlock_db();
}

leveldb::Status SSDBImpl::write(const leveldb::WriteOptions &options, leveldb::WriteBatch *batch) {
	return ldb->Write(options, batch);
}

int SSDBImpl::_update_global_version() {
	num_version_update++;
	global_version++;
	if(num_version_update <= version_update_threshold) {
		/* there is nothing to do, return ASAP */
		return 1;
	}
	/* record and reset counter */
	num_version_update = 0;
	int ret = this->raw_set(global_version_key(), str(global_version));
	if(ret != 1) {
		log_error("update global version failed");
		return -1;
	}
	return 1;
}

int SSDBImpl::new_version(const Bytes &key, char t, uint64_t *version) {
	int ret = _update_global_version();
	if (ret == -1) {
		log_error("update global version failed");
		return -1;
	}
	std::string k = encode_version_key(key);
	std::string v = encode_version(t, global_version);
	ret = this->raw_set(k, v);
	if(ret != 1) {
		log_error("new version failed, key:%s", key.String().c_str());
		return -1;
	}
	*version = global_version;
	return 1;
}

int SSDBImpl::get_version(const Bytes &key, char *t, uint64_t *version, const leveldb::Snapshot *snapshot) {
	std::string k = encode_version_key(key);
	std::string v;
	int ret = this->raw_get(k, &v, snapshot);
	if(ret == -1) {
		log_error("get version failed, key:%s", key.String().c_str());
		return -1;
	}
	if(ret == 0) {
		return 0;
	}

	char version_type;
	ret = decode_version(v, &version_type, version);
	if(ret != 0) {
		log_error("decode version failed");
		return -1;
	}
	*t = version_type;
	return 1;
}

int SSDBImpl::del(const Bytes &key, Transaction &trans) {
	trans.begin();
	/* delete version key */
	uint64_t version;
	char t;
	int ret = this->get_version(key, &t, &version);
	if(ret == -1) {
		return -1;
	}
	if(ret == 0) {
		return 0;
	}
	trans.del(encode_version_key(key));
	trans.put(encode_deprecated_key(key, t, version), "");
	Transaction::Status s = trans.commit();
	if(!s.ok()){
		log_error("delete commit failed");
		return -1;
	}
	return 1;
}

int SSDBImpl::raw_size(const Bytes &key, int64_t *size) {
	std::string value;
	int found = this->raw_get(key, &value);
	if(found == -1) {
		return -1;
	}
	if(found == 0) {
		*size = 0;
		return 0;
	}
	if(value.size() != sizeof(int64_t)) {
		*size = 0;
		return 0;
	}
	*size = *(int64_t *)value.data();
	return 0;
}

int SSDBImpl::incr_raw_size(const Bytes &key, int64_t incr, int64_t *size, Transaction &trans) {
	int ret = this->raw_size(key, size);
	if(ret != 0) {
		return -1;
	}
	*size += incr;
	if(*size == 0) {
		trans.del(key);
	} else {
		trans.put(key, Bytes((char*)size, sizeof(int64_t)));
	}
	return 0;
}

Iterator* SSDBImpl::keys(int16_t slot) {
	std::string start;
	start.append(SSDB_VERSION_KEY_PREFIX, sizeof(SSDB_VERSION_KEY_PREFIX));
	start.append((char*)&slot, sizeof(slot));

	std::string end;
	end.append(SSDB_VERSION_KEY_PREFIX, sizeof(SSDB_VERSION_KEY_PREFIX));
	end.append(1, '\xff');
	end.append((char*)&slot, sizeof(slot));
	return this->iterator(start, end, UINT_MAX);
}

std::string SSDBImpl::get_name() {
	return this->name;
}

/* GC */
static void *ssdb_gc_thread(void *arg) {
	pthread_detach(pthread_self());
	SSDBImpl *ssdb = (SSDBImpl *)arg;
	std::string name = ssdb->get_name();
	std::string procname = "gc_" + name;
	SET_PROC_NAME(procname.c_str());
	std::string start = encode_deprecated_key("", DataType::MIN_PREFIX, 0);
	std::string end = encode_deprecated_key("\xff", DataType::MAX_PREFIX, UINT_MAX);
	while(true) {
		uint64_t count = 0;
		Iterator *it = ssdb->iterator(start, end, UINT_MAX);
		if(it != NULL) {
			while(it->next()) {
				std::string key;
				uint64_t version;
				char t;
				if(decode_deprecated_key(it->key(), &key, &t, &version) == -1) {
					log_error("gc decode deprecated key failed: %s", hexmem((it->key()).data(), (it->key()).size()).c_str());
				}
				Transaction trans(ssdb, Bytes());
				switch(t) {
					case DataType::KV:
						ssdb->raw_del(encode_kv_key(key, version));
						log_debug("gc %s delete kv %s", name.c_str(),  key.c_str());
						count ++;
						break;
					case DataType::HASH:
						ssdb->hclear(key, trans, version);
						log_debug("gc %s delete hash %s", name.c_str(),  key.c_str());
						count ++;
						break;
					case DataType::SET:
						ssdb->sclear(key, trans, version);
						log_debug("gc %s delete set %s", name.c_str(),  key.c_str());
						count ++;
						break;
					case DataType::ZSET:
						ssdb->zclear(key, trans, version);
						log_debug("gc %s delete sorted set %s", name.c_str(), key.c_str());
						count ++;
						break;
					case DataType::QUEUE:
						log_debug("gc %s delete queue %s", name.c_str(), key.c_str());
						ssdb->qclear(key, trans, version);
						count ++;
						break;
					default:
						log_error("gc %s unknown type: %s", name.c_str(), hexmem((it->key()).data(), (it->key()).size()).c_str());
				}

				/* delete record */
				ssdb->raw_del(it->key());
			}
		}
		SAFE_DELETE(it);
		if(count == 0) {
			/* pause for a while */
			sleep(60);
		} else {
			/* give some time tp clean up low level data */
			sleep(10);
		}
		log_debug("gc %s loop done, clean %" PRIu64 " keys", name.c_str(), count);
	}
	return NULL;
}
