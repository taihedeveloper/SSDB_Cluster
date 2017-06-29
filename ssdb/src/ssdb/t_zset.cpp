/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <limits.h>
#include "t_zset.h"
#include "version.h"
#include "concurrent.h"
#include <sstream>

static const char *SSDB_SCORE_MIN		= "-9223372036854775808";
static const char *SSDB_SCORE_MAX		= "+9223372036854775807";

// for test
#ifdef BAIDU_TEST
static void print_ascii(const std::string &str);
#endif

static int incr_zsize(SSDBImpl *ssdb, const Bytes &key, int64_t incr, Transaction &trans, uint64_t version) {
	std::string zskey = encode_zsize_key(key, version);
	int64_t size;
	if(ssdb->incr_raw_size(zskey, incr, &size, trans) == -1) {
		return -1;
	}
	if(size == 0) {
		trans.del(zskey);
		trans.del(encode_version_key(key));
	}
	if(size < 0) {
		log_error("%s unexpected size: %" PRId64, hexmem(zskey.data(), zskey.size()).c_str(), size);
		return -1;
	}
	return 0;
}

static std::string filter_score(const Bytes &score){
	int64_t s = score.Int64();
	return str(s);
}

/* retval -1: error, 0: item updated, 1: new item inserted */
int SSDBImpl::zset(const Bytes &key, const Bytes &field, const Bytes &score, Transaction &trans, uint64_t version){
	std::string new_score = filter_score(score);
	std::string old_score;
	std::string zkey = encode_zset_key(key, field, version);
	int found = this->raw_get(zkey, &old_score);
	if(found == -1) {
		return -1;
	}

	trans.begin();
	if(found == 0 || old_score != new_score){
		std::string k0, k1, k2;

		if(found){
			// delete zscore key
			k1 = encode_zscore_key(key, field, old_score, version);
			trans.del(k1);
		}

		// add zscore key
		k2 = encode_zscore_key(key, field, new_score, version);
		trans.put(k2, "");

		// update zset
		k0 = encode_zset_key(key, field, version);
		trans.put(k0, new_score);

		if(found == 0) {
			if (incr_zsize(this, key, 1, trans, version) == -1) {
				return -1;
			}
		}

		Transaction::Status s = trans.commit();
		if(!s.ok()) {
			return -1;
		}
	}
	return found ? 0 : 1;
}

int SSDBImpl::zdel(const Bytes &key, const Bytes &field, Transaction &trans, uint64_t version){
	std::string zkey = encode_zset_key(key, field, version);
	std::string old_score;
	int found = this->raw_get(zkey, &old_score);
	if(found == -1){
		return -1;
	}
	if(found == 0) {
		return 0;
	}

	// delete zscore key
	std::string ss = encode_zscore_key(key, field, old_score, version);
	trans.del(ss);
	trans.del(zkey);
	if(incr_zsize(this, key, -1, trans, version) == -1) {
		return -1;
	}

	Transaction::Status s = trans.commit();
	if(!s.ok()) {
		return -1;
	}
	return 1;
}

/* retval 0: updated 1: new -1: error */
int SSDBImpl::zincr(const Bytes &key, const Bytes &field, int64_t by, int64_t *new_val, Transaction &trans, uint64_t version) {
	trans.begin();

	std::string zkey = encode_zset_key(key, field, version);
	std::string value;
	int ret = this->raw_get(zkey, &value);
	if(ret == -1) {
		return -1;
	}
	if(ret == 0) {
		*new_val = by;
		if(incr_zsize(this, key, 1 , trans, version) == -1) {
			return -1;
		}
	} else {
		trans.del(encode_zscore_key(key, field, value, version));
		*new_val = str_to_int64(value) + by;
	}
	std::string score = str(*new_val);
	trans.put(zkey, score);
	trans.put(encode_zscore_key(key, field, score, version), "");

	Transaction::Status s = trans.commit();
	if(!s.ok()) {
		return -1;
	}
	return 1;
}

int64_t SSDBImpl::zsize(const Bytes &key, uint64_t version){
	std::string zskey = encode_zsize_key(key, version);
	int64_t size;
	int ret = this->raw_size(zskey, &size);
	if(ret == -1) {
		return -1;
	}
	if(size < 0) {
		log_error("%s unexpected size: %" PRId64, hexmem(zskey.data(), zskey.size()).c_str(), size);
		return -1;
	}
	return size;
}

int SSDBImpl::zget(const Bytes &key,
	   	const Bytes &field, 
		std::string *score, 
		uint64_t version, 
		const leveldb::Snapshot *snapshot){
	std::string zkey = encode_zset_key(key, field, version);
	return this->raw_get(zkey, score, snapshot);
}

static ZIterator* ziterator(
	SSDBImpl *ssdb,
	const Bytes &key, const Bytes &key_start,
	const Bytes &score_start, const Bytes &score_end,
	uint64_t limit, Iterator::Direction direction, uint64_t version)
{
	if(direction == Iterator::FORWARD){
		std::string start, end;
		if(score_start.empty()){
			start = encode_zscore_key(key, key_start, SSDB_SCORE_MIN, version);
		}else{
			start = encode_zscore_key(key, key_start, score_start, version);
		}
		if(score_end.empty()){
			end = encode_zscore_key(key, "\xff", SSDB_SCORE_MAX, version);
		}else{
			end = encode_zscore_key(key, "\xff", score_end, version);
		}
		return new ZIterator(ssdb->iterator(start, end, limit), key);
	}else{
		std::string start, end;
		if(score_start.empty()){
			start = encode_zscore_key(key, key_start, SSDB_SCORE_MAX, version);
		}else{
			if(key_start.empty()){
				start = encode_zscore_key(key, "\xff", score_start, version);
			}else{
				start = encode_zscore_key(key, key_start, score_start, version);
			}
		}
		if(score_end.empty()){
			end = encode_zscore_key(key, "", SSDB_SCORE_MIN, version);
		}else{
			end = encode_zscore_key(key, "", score_end, version);
		}
		return new ZIterator(ssdb->rev_iterator(start, end, limit), key);
	}
}

int64_t SSDBImpl::zrank(const Bytes &key, const Bytes &field, uint64_t version){
	ZIterator *it =	ziterator(this, key, "", "", "", INT_MAX, Iterator::FORWARD, version);
	int64_t ret = 0;
	while(true){
		if(it->next() == false){
			ret = -1;
			break;
		}
		if(field == it->field){
			break;
		}
		ret ++;
	}
	delete it;
	return ret;
}

int64_t SSDBImpl::zrrank(const Bytes &key, const Bytes &field, uint64_t version){
	ZIterator *it = ziterator(this, key, "", "", "", INT_MAX, Iterator::BACKWARD, version);
	int64_t ret = 0;
	while(true){
		if(it->next() == false){
			ret = -1;
			break;
		}
		if(field == it->field){
			break;
		}
		ret ++;
	}
	delete it;
	return ret;
}

ZIterator* SSDBImpl::zrange(const Bytes &key, int64_t start, int64_t stop, uint64_t version) {
	uint64_t limit = 0;
	uint64_t offset = 0;

	if(start < 0 || stop < 0) {
		int64_t size = zsize(key, version);
		if(start < 0) {
			start += size;
		}
		if(start < 0) {
			start = 0;
		}
		if(stop < 0) {
			stop += size;
		}
		if(stop < 0) {
			return NULL;
		}
		limit = stop - start + 1;
		if(limit <= 0 ) {
			return NULL;
		}
		offset = start;
		return zrange(key, offset, limit, version);
	} else {
		limit = stop - start + 1;
		if(limit <= 0) {
			return NULL;
		}
		offset = start;
		return zrange(key, offset, limit, version);
	}
}

ZIterator* SSDBImpl::zrrange(const Bytes &key, int64_t start, int64_t stop, uint64_t version) {
	uint64_t limit = 0;
	uint64_t offset = 0;

	if(start < 0 || stop < 0) {
		int64_t size = zsize(key, version);
		if(start < 0) {
			start += size;
		}
		if(start < 0) {
			return NULL;
		}
		if(stop < 0) {
			stop += size;
		}
		if(stop < 0) {
			return NULL;
		}
		limit = stop - start + 1;
		if(limit <= 0) {
			return NULL;
		}
		offset = start;
		return zrrange(key, offset, limit, version);
	} else {
		limit = stop - start + 1;
		if(limit <= 0) {
			return NULL;
		}
		offset = start;
		return zrrange(key, offset, limit, version);
	}
}

ZIterator* SSDBImpl::zrange(const Bytes &key, uint64_t offset, uint64_t limit, uint64_t version){
	limit = offset + limit;
	ZIterator *it = ziterator(this, key, "", "", "", limit, Iterator::FORWARD, version);
	it->skip(offset);
	return it;
}

ZIterator* SSDBImpl::zrrange(const Bytes &key, uint64_t offset, uint64_t limit, uint64_t version){
	limit = offset + limit;
	ZIterator *it = ziterator(this, key, "", "", "", limit, Iterator::BACKWARD, version);
	it->skip(offset);
	return it;
}

ZIterator* SSDBImpl::zscan(const Bytes &key, const Bytes &field,
		const Bytes &score_start, const Bytes &score_end, uint64_t limit, uint64_t version)
{
	std::string score;
	// if only key is specified, load its value
	if(!key.empty() && score_start.empty()){
		if(this->zget(key, field, &score, version) == -1) {
			return NULL;
		}
	}else{
		score = score_start.String();
	}
	return ziterator(this, key, field, score, score_end, limit, Iterator::FORWARD, version);
}

ZIterator* SSDBImpl::zrscan(const Bytes &key, const Bytes &field,
		const Bytes &score_start, const Bytes &score_end, uint64_t limit, uint64_t version)
{
	std::string score;
	// if only key is specified, load its value
	if(!key.empty() && score_start.empty()){
		if(this->zget(key, field, &score, version) == -1) {
			return NULL;
		}
	}else{
		score = score_start.String();
	}
	return ziterator(this, key, field, score, score_end, limit, Iterator::BACKWARD, version);
}

int64_t SSDBImpl::zclear(const Bytes &key, Transaction &trans, uint64_t version) {
	int64_t count = 0;
	uint64_t offset = 0;
	uint64_t limit = UINT_MAX;
	ZIterator *it = this->zrange(key, offset, limit, version);
	while(it->next()) {
		if(this->zdel(key, it->field, trans, version) == -1) {
			delete it;
			return -1;
		}
		count ++;
	}
	delete it;
	return count;
}

static void get_znames(Iterator *it, std::vector<std::string> *list){
	/*while(it->next()){
		Bytes ks = it->key();
		//dump(ks.data(), ks.size());
		if(ks.data()[0] != DataType::ZSIZE){
			break;
		}
		std::string n;
		if(decode_zsize_key(ks, &n) == -1){
			continue;
		}
		list->push_back(n);
	}*/
}

int SSDBImpl::zlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;

	/*start = encode_zsize_key(name_s);
	if(!name_e.empty()){
		end = encode_zsize_key(name_e);
	}

	Iterator *it = this->iterator(start, end, limit);
	get_znames(it, list);
	delete it;*/
	return 0;
}

int SSDBImpl::zrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;

	/*start = encode_zsize_key(name_s);
	if(name_s.empty()){
		start.append(1, 255);
	}
	if(!name_e.empty()){
		end = encode_zsize_key(name_e);
	}

	Iterator *it = this->rev_iterator(start, end, limit);
	get_znames(it, list);
	delete it;*/
	return 0;
}

#ifdef BAIDU_TEST
static void print_ascii(const std::string &str) {
    std::stringstream ss;
    for(int i = 0; i < str.size(); ++i) {
        ss << int(uint8_t(str[i]));
        ss << " ";
    }
    log_error("org: %s, ascii: %s", str.c_str(), ss.str().c_str());
}
#endif

