/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <limits.h>
#include "t_set.h"
#include <sstream>

static int incr_ssize(SSDBImpl *ssdb, const Bytes &key, int64_t incr, Transaction &trans, uint64_t version) {
	std::string sskey = encode_ssize_key(key, version);
	int64_t size;
	if(ssdb->incr_raw_size(sskey, incr, &size, trans) == -1) {
		return -1;
	}
	if(size == 0) {
		trans.del(sskey);
	} else if(size < 0) {
		log_error("%s unexpected size: %" PRId64, hexmem(sskey.data(), sskey.size()).c_str(), size);
		return -1;
	}
	return 0;
}

int SSDBImpl::sget(const Bytes &key, const Bytes &elem, uint64_t version) {
	std::string skey = encode_set_key(key, elem, version);
	std::string value;
	return this->raw_get(skey, &value);
}

int SSDBImpl::sset(const Bytes &key, const Bytes &elem, Transaction &trans, uint64_t version) {
	trans.begin();

	int found = this->sget(key, elem, version);
	if(found == -1) {
		return -1;
	}
	if(found != 0) {
		return 0;
	}
	/* add elem */
	trans.put(encode_set_key(key, elem, version), "");
	if(incr_ssize(this, key, 1, trans, version) == -1) {
		return -1;
	}

	Transaction::Status s = trans.commit();
	if(!s.ok()) {
		log_error("sset error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::sdel(const Bytes &key, const Bytes &elem, Transaction &trans, uint64_t version) {
	trans.begin();
	int found = this->sget(key, elem, version);
	if(found == -1) {
		return -1;
	}
	if(found == 0) {
		return 0;
	}

	trans.del(encode_set_key(key, elem, version));
	if(incr_ssize(this, key, -1, trans, version) == -1) {
		return -1;
	}

	Transaction::Status s = trans.commit();
	if(!s.ok()) {
		log_error("sdel error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}
int64_t SSDBImpl::ssize(const Bytes &key, uint64_t version){
	std::string sskey = encode_ssize_key(key, version);
	int64_t size;
	int ret = this->raw_size(sskey, &size);
	if(ret == -1) {
		return -1;
	}
	if(size < 0) {
		log_error("%s unexpected size: %" PRId64, hexmem(sskey.data(), sskey.size()).c_str(), size);
		return -1;
	}
	return size;
}

static SIterator* siterator(
		SSDBImpl *ssdb,
		const Bytes &key,
		const Bytes &elem_start,
		uint64_t limit,
		Iterator::Direction direction,
		uint64_t version) {
	if(direction == Iterator::FORWARD) {
		std::string start = encode_set_key(key, elem_start, version);
		std::string end = encode_set_key(key, "\xff", version);
		return new SIterator(ssdb->iterator(start, end, limit), key);
	} else {
		std::string start = encode_set_key(key, elem_start, version);
		std::string end = encode_set_key(key, "", version);
		return new SIterator(ssdb->rev_iterator(start, end, limit), key);
	}
}

SIterator* SSDBImpl::sscan(const Bytes &key, const Bytes &elem, uint64_t limit, uint64_t version) {
	return siterator(this, key, elem, limit, Iterator::FORWARD, version);
}

SIterator* SSDBImpl::srscan(const Bytes &key, const Bytes &elem, uint64_t limit, uint64_t version) {
	return siterator(this, key, elem, limit, Iterator::BACKWARD, version);
}

int64_t SSDBImpl::sclear(const Bytes &key, Transaction &trans, uint64_t version) {
	int64_t count = 0;
	uint64_t limit = UINT_MAX;
	SIterator *it = this->sscan(key, "", limit, version);
	while(it->next()) {
		int ret = this->sdel(key, it->elem, trans, version);
		if(ret == -1) {
			delete it;
			return -1;
		}
		count ++;
	}
	delete it;
	return count;
}
