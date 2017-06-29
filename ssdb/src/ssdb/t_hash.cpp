/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "t_hash.h"
#include "version.h"
#include "concurrent.h"

static int incr_hsize(SSDBImpl *ssdb, const Bytes &key, int64_t incr, Transaction &trans, uint64_t version) {
	std::string hskey = encode_hsize_key(key, version);
	int64_t size;
	if(ssdb->incr_raw_size(hskey, incr, &size, trans) == -1) {
		return -1;
	}
	if(size == 0) {
		trans.del(hskey);
		trans.del(encode_version_key(key));
	} else if(size < 0) {
		log_error("%s unexpected size: %" PRId64, hexmem(hskey.data(), hskey.size()).c_str(), size);
		return -1;
	}
	return 0;
}

/* retval -1: error, 0: item updated, 1: new item inserted */
int SSDBImpl::hset(const Bytes &key, const Bytes &field, const Bytes &val, Transaction &trans, uint64_t version){
	trans.begin();

	std::string hkey = encode_hash_key(key, field, version);
	std::string value;
	int ret = this->raw_get(hkey, &value);
	if(ret == -1) {
		return -1;
	}
	if(ret == 0) {
		trans.put(Bytes(hkey.data(), hkey.size()), val);
		if(incr_hsize(this, key, 1, trans, version) == -1) {
			return -1;
		}
	} else {
		if(value != val) {
			trans.put(Bytes(hkey.data(), hkey.size()), val);
		}
	}

	Transaction::Status s = trans.commit();
	if(!s.ok()) {
		return -1;
	}
	return ret == 0 ? 1 : 0;
}

/* retval 0: not found 1: ok -1: error */
int SSDBImpl::hdel(const Bytes &key, const Bytes &field, Transaction &trans, uint64_t version){
	trans.begin();

	std::string hkey = encode_hash_key(key, field, version);
	std::string value;
	int found = this->raw_get(hkey, &value);
	if(found == -1) {
		return -1;
	}
	if(found == 0) {
		return 0;
	}

	trans.del(hkey);
	if(incr_hsize(this, key, -1, trans, version) == -1) {
		return -1;
	}

	Transaction::Status s = trans.commit();
	if(!s.ok()) {
		log_error("hdel error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

/* retval 0: updated 1: new  -1: error */
int SSDBImpl::hincr(const Bytes &key, const Bytes &field, int64_t by, int64_t *new_val, Transaction &trans, uint64_t version) {
	trans.begin();

	std::string hkey = encode_hash_key(key, field, version);
	std::string value;
	int ret = this->raw_get(hkey, &value);
	if(ret == -1) {
		return -1;
	}
	if(ret == 0) {
		*new_val = by;
		if(incr_hsize(this, key, 1, trans, version) == -1) {
			return -1;
		}
	} else {
		*new_val = str_to_int64(value) + by;
		if(errno == EINVAL) {
			return 0;
		}
	}
	trans.put(hkey, str(*new_val));

	Transaction::Status s = trans.commit();
	if(!s.ok()) {
		return -1;
	}
	return 1;
}

int64_t SSDBImpl::hsize(const Bytes &key, uint64_t version){
	std::string hskey = encode_hsize_key(key, version);
	int64_t size;
	int ret = this->raw_size(hskey, &size);
	if(ret == -1) {
		return -1;
	}
	if(size < 0) {
		log_error("%s unexpected size: %" PRId64, hexmem(hskey.data(), hskey.size()).c_str(), size);
		return -1;
	}
	return size;
}

int64_t SSDBImpl::hclear(const Bytes &key, Transaction &trans, uint64_t version){
	int64_t count = 0;
	uint64_t limit = UINT_MAX;
	HIterator *it = this->hscan(key, "", "", limit, version);
	if(it == NULL) {
		return 0;
	} 
	while(it->next()) {
		int ret = this->hdel(key, it->field, trans, version);
		if(ret == -1) {
			delete it;
			return -1;
		}
		count ++;
	}
	delete it;
	return count;
}

int SSDBImpl::hget(const Bytes &key, const Bytes &field, std::string *val, uint64_t version){
	std::string hkey = encode_hash_key(key, field, version);
	return this->raw_get(hkey, val);
}

HIterator* SSDBImpl::hscan(const Bytes &key, const Bytes &start, const Bytes &end, uint64_t limit, uint64_t version){
	std::string key_start, key_end;

	key_start = encode_hash_key(key, start, version);
	if(!end.empty()){
        log_error("end empty");
		key_end = encode_hash_key(key, end, version);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");
	return new HIterator(this->iterator(key_start, key_end, limit), key);
}

HIterator* SSDBImpl::hrscan(const Bytes &key, const Bytes &start, const Bytes &end, uint64_t limit, uint64_t version){
	std::string key_start, key_end;

    if(!start.empty()) {
	    key_start = encode_hash_key(key, start, version);
    } else {
		key_start = encode_hash_key(key, "\xff", version);
	}
	if(!end.empty()){
		key_end = encode_hash_key(key, end, version);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new HIterator(this->rev_iterator(key_start, key_end, limit), key);
}

static void get_hnames(Iterator *it, std::vector<std::string> *list){
	/*while(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] != DataType::HSIZE){
			break;
		}
		std::string n;
		if(decode_hsize_key(ks, &n) == -1){
			continue;
		}
		list->push_back(n);
	}*/
}

int SSDBImpl::hlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	/*std::string start;
	std::string end;

	start = encode_hsize_key(name_s);
	if(!name_e.empty()){
		end = encode_hsize_key(name_e);
	}

	Iterator *it = this->iterator(start, end, limit);
	get_hnames(it, list);
	delete it;*/
	return 0;
}

int SSDBImpl::hrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;

	/*start = encode_hsize_key(name_s);
	if(name_s.empty()){
		start.append(1, 255);
	}
	if(!name_e.empty()){
		end = encode_hsize_key(name_e);
	}

	Iterator *it = this->rev_iterator(start, end, limit);
	get_hnames(it, list);
	delete it;*/
	return 0;
}
