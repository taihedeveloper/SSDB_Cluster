/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "t_kv.h"
#include "version.h"

int SSDBImpl::set(const Bytes &key, const Bytes &val, Transaction &trans, uint64_t version){
	trans.begin();

	std::string kkey = encode_kv_key(key, version);
	trans.put(Bytes(kkey.data(), kkey.size()), val);
	Transaction::Status s = trans.commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::incr(const Bytes &key, int64_t by, int64_t *new_val, Transaction &trans, uint64_t version){
	trans.begin();

	std::string old;
	std::string kkey = encode_kv_key(key, version);
	int ret = this->get(key, &old, version);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		*new_val = by;
	}else{
		*new_val = str_to_int64(old) + by;
		if(errno != 0){
			return -1;
		}
	}

	trans.put(kkey, str(*new_val));
	Transaction::Status s = trans.commit();
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::get(const Bytes &key, std::string *val, uint64_t version){
	std::string kkey = encode_kv_key(key, version);
	leveldb::Status s = ldb->Get(leveldb::ReadOptions(), kkey, val);
	if(s.IsNotFound()) {
		return 0;
	}
	if(!s.ok()) {
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::mget(const std::vector<Bytes> &key, std::vector<std::string> *val, std::vector<uint64_t> version, const leveldb::Snapshot *snapshot){
	leveldb::ReadOptions options = leveldb::ReadOptions();
	options.snapshot = snapshot;

	if (key.size() != version.size()) {
		log_error("params error: key.size() != version.size()");
		return 0;
	}

	for (int i = 0; i < key.size(); ++i)
	{
		std::string value;
		std::string kkey = encode_kv_key(key[i], version[i]);
		leveldb::Status s = ldb->Get(options, kkey, &value);
		if(s.IsNotFound()) {
			continue;
		}
		if(!s.ok()) {
			log_error("get error: %s", s.ToString().c_str());
			return -1;
		}
		val->push_back(key[i].String());
		val->push_back(value);
	}

	return 1;
}


KIterator* SSDBImpl::scan(const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;
	key_start = encode_kv_key(start, 0);
	if(end.empty()){
		key_end = "";
	}else{
		key_end = encode_kv_key(end, UINT_MAX);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->iterator(key_start, key_end, limit));
}

KIterator* SSDBImpl::rscan(const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;

	key_start = encode_kv_key(start, UINT_MAX);
	if(start.empty()){
		key_start.append(1, 255);
	}
	if(!end.empty()){
		key_end = encode_kv_key(end, 0);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->rev_iterator(key_start, key_end, limit));
}

int SSDBImpl::setbit(const Bytes &key, int bitoffset, int on, Transaction &trans, uint64_t version){
	trans.begin();

	std::string val;
	std::string kkey = encode_kv_key(key, version);
	int ret = this->raw_get(kkey, &val);
	if(ret == -1) {
		return -1;
	}

	/* the sequence isn't compatible with redis, rewrite it
	 * ssdb <-
	 * redis ->
	 */
	//int len = bitoffset / 8;
	//int bit = bitoffset % 8;
	int len = bitoffset >> 3;
	int bit = 7 - (bitoffset & 0x7);
	if(len >= val.size()){
		val.resize(len + 1, 0);
	}
	int orig = val[len] & (1 << bit);
	if(on == 1){
		val[len] |= (1 << bit);
	}else{
		val[len] &= ~(1 << bit);
	}

	trans.put(Bytes(kkey.data(), kkey.size()), val);
	Transaction::Status s = trans.commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return orig;
}

int SSDBImpl::getbit(const Bytes &key, int bitoffset, uint64_t version){
	std::string val;
	int ret = this->get(key, &val, version);
	if(ret == -1){
		return -1;
	}

	int len = bitoffset / 8;
	int bit = 7 - (bitoffset & 0x7);
	if(len >= val.size()){
		return 0;
	}
	return val[len] & (1 << bit);
}
