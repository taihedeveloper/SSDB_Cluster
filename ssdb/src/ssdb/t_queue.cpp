/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "t_queue.h"
#include "version.h"
#include "../util/hash.h"

static int qdel_one(SSDBImpl *ssdb, const Bytes &name, uint64_t seq, Transaction &trans, uint64_t version){
	std::string key = encode_qitem_key(name, seq, version);
	trans.del(key);
	return 0;
}

static int qset_one(SSDBImpl *ssdb, const Bytes &name, uint64_t seq, const Bytes &item, Transaction &trans, uint64_t version){
	std::string key = encode_qitem_key(name, seq, version);
	trans.put(key, item);
	return 0;
}

static int64_t incr_qsize(SSDBImpl *ssdb, const Bytes &key, int64_t incr, Transaction &trans, uint16_t version) {
	std::string qskey = encode_qsize_key(key, version);
	int64_t size;
	if(ssdb->incr_raw_size(qskey, incr, &size, trans) == -1) {
		return -1;
	}
	if(size == 0) {
		trans.del(qskey);
		trans.del(encode_version_key(key));
		qdel_one(ssdb, key, QFRONT_SEQ, trans, version);
		qdel_one(ssdb, key, QBACK_SEQ, trans, version);
	} else if(size < 0) {
		log_error("%s unexpected size: %" PRIu64, hexmem(qskey.data(), qskey.size()).c_str(), size);
		return -1;
	}
	return size;
}

static int qget_by_seq(leveldb::DB* db, const Bytes &key, uint64_t seq, std::string *val, uint64_t version){
	std::string qkey = encode_qitem_key(key, seq, version);
	leveldb::Status s;

	s = db->Get(leveldb::ReadOptions(), qkey, val);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		log_error("Get() error!");
		return -1;
	}else{
		return 1;
	}
}

static int qget_uint64(leveldb::DB* db, const Bytes &key, uint64_t seq, uint64_t *ret, uint64_t version){
	std::string val;
	*ret = 0;
	int s = qget_by_seq(db, key, seq, &val, version);
	if(s == 1){
		if(val.size() != sizeof(uint64_t)){
			return -1;
		}
		*ret = *(uint64_t *)val.data();
	}
	return s;
}

int64_t SSDBImpl::qsize(const Bytes &key, uint64_t version){
	std::string qskey = encode_qsize_key(key, version);
	int64_t size;
	int ret = this->raw_size(qskey, &size);
	if(ret == -1) {
		return -1;
	}
	if(size < 0) {
		log_error("%s unexpected size: %" PRIu64, hexmem(qskey.data(), qskey.size()).c_str(), size);
		return -1;
	}
	return size;
}

/* retval 0: empty queue, 1: item peeked, -1: error */
int SSDBImpl::qfront(const Bytes &key, std::string *item, uint64_t version){
	uint64_t seq;
	int ret = qget_uint64(this->ldb, key, QFRONT_SEQ, &seq, version);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}
	return qget_by_seq(this->ldb, key, seq, item, version);
}

/* retval 0: empty queue, 1: item peeked, -1: error */
int SSDBImpl::qback(const Bytes &key, std::string *item, uint64_t version){
	uint64_t seq;
	int ret = qget_uint64(this->ldb, key, QBACK_SEQ, &seq, version);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}
	ret = qget_by_seq(this->ldb, key, seq, item, version);
	return ret;
}

/* retval 0: index out of range, -1: error, 1: ok */
int SSDBImpl::qset(const Bytes &key, int64_t index, const Bytes &item, Transaction &trans, uint64_t version){
	trans.begin();

	int64_t size = this->qsize(key, version);
	if(size == -1){
		return -1;
	}
	if(index >= size || index < -size){
		return 0;
	}

	int ret;
	uint64_t seq;
	if(index >= 0){
		ret = qget_uint64(this->ldb, key, QFRONT_SEQ, &seq, version);
		seq += index;
	}else{
		ret = qget_uint64(this->ldb, key, QBACK_SEQ, &seq, version);
		seq += index + 1;
	}
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}

	ret = qset_one(this, key, seq, item, trans, version);
	if(ret == -1){
		return -1;
	}

	Transaction::Status s = trans.commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}
	return 1;
}

int64_t SSDBImpl::_qpush(const Bytes &key, const Bytes &item, uint64_t front_or_back_seq, Transaction &trans, uint64_t version){
	trans.begin();

	int ret;
	// generate seq
	uint64_t seq;
	ret = qget_uint64(this->ldb, key, front_or_back_seq, &seq, version);
	if(ret == -1){
		return -1;
	}
	// update front and/or back
	if(ret == 0){
		seq = QITEM_SEQ_INIT;
		ret = qset_one(this, key, QFRONT_SEQ, Bytes(&seq, sizeof(seq)), trans, version);
		if(ret == -1){
			return -1;
		}
		ret = qset_one(this, key, QBACK_SEQ, Bytes(&seq, sizeof(seq)), trans, version);
	}else{
		seq += (front_or_back_seq == QFRONT_SEQ)? -1 : +1;
		ret = qset_one(this, key, front_or_back_seq, Bytes(&seq, sizeof(seq)), trans, version);
	}
	if(ret == -1){
		return -1;
	}
	if(seq <= QITEM_MIN_SEQ || seq >= QITEM_MAX_SEQ){
		log_info("queue is full, seq: %" PRIu64 " out of range", seq);
		return -1;
	}

	// prepend/append item
	ret = qset_one(this, key, seq, item, trans, version);
	if(ret == -1){
		return -1;
	}

	// update size
	int64_t size = incr_qsize(this, key, 1, trans, version);
	if(size == -1){
		return -1;
	}

	Transaction::Status s = trans.commit();
	if(!s.ok()){
		log_error("Write error! %s", s.ToString().c_str());
		return -1;
	}
	return size;
}

int64_t SSDBImpl::qpush_front(const Bytes &key, const Bytes &item, Transaction &trans, uint64_t version){
	return _qpush(key, item, QFRONT_SEQ, trans, version);
}

int64_t SSDBImpl::qpush_back(const Bytes &key, const Bytes &item, Transaction &trans, uint64_t version){
	return _qpush(key, item, QBACK_SEQ, trans, version);
}

int SSDBImpl::_qpop(const Bytes &key, std::string *item, uint64_t front_or_back_seq, Transaction &trans, uint64_t version){
	trans.begin();

	int ret;
	uint64_t seq;
	ret = qget_uint64(this->ldb, key, front_or_back_seq, &seq, version);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}

	ret = qget_by_seq(this->ldb, key, seq, item, version);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}

	// delete item
	ret = qdel_one(this, key, seq, trans, version);
	if(ret == -1){
		return -1;
	}

	// update size
	int64_t size = incr_qsize(this, key, -1, trans, version);
	if(size == -1){
		return -1;
	}

	// update front
	if(size > 0){
		seq += (front_or_back_seq == QFRONT_SEQ)? +1 : -1;
		//log_debug("seq: %" PRIu64 ", ret: %d", seq, ret);
		ret = qset_one(this, key, front_or_back_seq, Bytes(&seq, sizeof(seq)), trans, version);
		if(ret == -1){
			return -1;
		}
	}

	Transaction::Status s = trans.commit();
	if(!s.ok()){
		log_error("Write error! %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

// @return 0: empty queue, 1: item popped, -1: error
int SSDBImpl::qpop_front(const Bytes &name, std::string *item, Transaction &trans, uint64_t version){
	return _qpop(name, item, QFRONT_SEQ, trans, version);
}

int SSDBImpl::qpop_back(const Bytes &name, std::string *item, Transaction &trans, uint64_t version){
	return _qpop(name, item, QBACK_SEQ, trans, version);
}

int64_t SSDBImpl::qclear(const Bytes &key, Transaction &trans, uint64_t version) {
	int64_t count = 0;
	while(true) {
		std::string item;
		int ret = this->qpop_front(key, &item, trans, version);
		if (ret == 0) {
			break;
		}
		if (ret == -1) {
			return -1;
		}
		count += 1;
	}
	return count;
}

static void get_qnames(Iterator *it, std::vector<std::string> *list){
	/*while(it->next()){
		Bytes ks = it->key();
		//dump(ks.data(), ks.size());
		if(ks.data()[0] != DataType::QSIZE){
			break;
		}
		std::string n;
		if(decode_qsize_key(ks, &n) == -1){
			continue;
		}
		list->push_back(n);
	}*/
}

int SSDBImpl::qlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;

	/*start = encode_qsize_key(name_s);
	if(!name_e.empty()){
		end = encode_qsize_key(name_e);
	}

	Iterator *it = this->iterator(start, end, limit);
	get_qnames(it, list);
	delete it;*/
	return 0;
}

int SSDBImpl::qrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;

	/*start = encode_qsize_key(name_s);
	if(name_s.empty()){
		start.append(1, 255);
	}
	if(!name_e.empty()){
		end = encode_qsize_key(name_e);
	}

	Iterator *it = this->rev_iterator(start, end, limit);
	get_qnames(it, list);
	delete it;*/
	return 0;
}

int SSDBImpl::qfix(const Bytes &name, Transaction &trans){
	/*std::string key_s = encode_qitem_key(name, QITEM_MIN_SEQ - 1);
	std::string key_e = encode_qitem_key(name, QITEM_MAX_SEQ);

	bool error = false;
	uint64_t seq_min = 0;
	uint64_t seq_max = 0;
	uint64_t count = 0;
	Iterator *it = this->iterator(key_s, key_e, QITEM_MAX_SEQ);
	while(it->next()){
		//dump(it->key().data(), it->key().size());
		if(seq_min == 0){
			if(decode_qitem_key(it->key(), NULL, &seq_min) == -1){
				// or just delete it?
				error = true;
				break;
			}
		}
		if(decode_qitem_key(it->key(), NULL, &seq_max) == -1){
			error = true;
			break;
		}
		count ++;
	}
	delete it;
	if(error){
		return -1;
	}

	if(count == 0){
		trans.del(encode_qsize_key(name));
		qdel_one(this, name, QFRONT_SEQ, trans);
		qdel_one(this, name, QBACK_SEQ, trans);
	}else{
		trans.put(encode_qsize_key(name), std::string((char *)&count, sizeof(count)));
		qset_one(this, name, QFRONT_SEQ, Bytes(&seq_min, sizeof(seq_min)), trans);
		qset_one(this, name, QBACK_SEQ, Bytes(&seq_max, sizeof(seq_max)), trans);
	}

	Transaction::Status s = trans.commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}*/
	return 0;
}

int SSDBImpl::qslice(const Bytes &key, int64_t begin, int64_t end, uint64_t version, std::vector<std::string> *list)
{
	int ret;
	uint64_t seq_begin, seq_end;
	if(begin >= 0 && end >= 0){
		uint64_t tmp_seq;
		ret = qget_uint64(this->ldb, key, QFRONT_SEQ, &tmp_seq, version);
		if(ret != 1){
			return ret;
		}
		seq_begin = tmp_seq + begin;
		seq_end = tmp_seq + end;
	}else if(begin < 0 && end < 0){
		uint64_t tmp_seq;
		ret = qget_uint64(this->ldb, key, QBACK_SEQ, &tmp_seq, version);
		if(ret != 1){
			return ret;
		}
		seq_begin = tmp_seq + begin + 1;
		seq_end = tmp_seq + end + 1;
	}else{
		uint64_t f_seq, b_seq;
		ret = qget_uint64(this->ldb, key, QFRONT_SEQ, &f_seq, version);
		if(ret != 1){
			return ret;
		}
		ret = qget_uint64(this->ldb, key, QBACK_SEQ, &b_seq, version);
		if(ret != 1){
			return ret;
		}
		if(begin >= 0){
			seq_begin = f_seq + begin;
		}else{
			seq_begin = b_seq + begin + 1;
		}
		if(end >= 0){
			seq_end = f_seq + end;
		}else{
			seq_end = b_seq + end + 1;
		}
	}

	for(; seq_begin <= seq_end; seq_begin++){
		std::string item;
		ret = qget_by_seq(this->ldb, key, seq_begin, &item, version);
		if(ret == -1){
			return -1;
		}
		if(ret == 0){
			return 0;
		}
		list->push_back(item);
	}
	return 0;
}

int SSDBImpl::qget(const Bytes &key, int64_t index, std::string *item, uint64_t version){
	int ret;
	uint64_t seq;
	if(index >= 0){
		ret = qget_uint64(this->ldb, key, QFRONT_SEQ, &seq, version);
		seq += index;
	}else{
		ret = qget_uint64(this->ldb, key, QBACK_SEQ, &seq, version);
		seq += index + 1;
	}
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}

	ret = qget_by_seq(this->ldb, key, seq, item, version);
	return ret;
}

QueueSeqAlloc::QueueSeqAlloc(SSDB *meta, int16_t slotcount, uint64_t step)
: meta(meta), step(step) {

	uint16_t slot = slotcount;

	// init keys
	for (int i = 0; i < QUEUE_SEQ_CONCURRENT; i++) {
		front_seq_keys[i] = std::string(QUEUE_FRONT_SEQ) + str(i);
		front_seq_keys[i].append((char *)&slot, sizeof(uint16_t));

		back_seq_keys[i] = std::string(QUEUE_BACK_SEQ) + str(i);
		back_seq_keys[i].append((char *)&slot, sizeof(uint16_t));
	}

	// init front seqs
	for (int i = 0; i < QUEUE_SEQ_CONCURRENT; i++) {
		front_seqs[i].type = SeqRange::FRONT;

		std::string tmp;
		int ret = meta->raw_get(Bytes(front_seq_keys[i]), &tmp);
		if (ret == 0) {
			front_seqs[i].begin = QITEM_SEQ_INIT - 1;
		} else if (ret == 1) {
			assert (tmp.size() == sizeof(uint64_t));
			front_seqs[i].begin = *((uint64_t *)tmp.data()) - QUEUE_SAFE_INCREMENT;
		} else {
			log_error("get front seq failed.");
			assert (0);
		}

		// save end
		front_seqs[i].end = front_seqs[i].begin - step;
		ret = meta->raw_set(Bytes(front_seq_keys[i]),
				Bytes((char *)&front_seqs[i].end, sizeof(uint64_t)));
		if (ret < 0) {
			log_warn("save front seq failed.");
		}
	}

	// init back seqs
	for (int i = 0; i < QUEUE_SEQ_CONCURRENT; i++) {
		back_seqs[i].type = SeqRange::BACK;

		std::string tmp;
		int ret = meta->raw_get(Bytes(back_seq_keys[i]), &tmp);
		if (ret == 0) {
			back_seqs[i].begin = QITEM_SEQ_INIT;
		} else if (ret == 1) {
			assert (tmp.size() == sizeof(uint64_t));
			back_seqs[i].begin = *((uint64_t *)tmp.data()) + QUEUE_SAFE_INCREMENT;
		} else {
			log_error("get back seq failed.");
			assert(0);
		}

		// save end
		back_seqs[i].end = back_seqs[i].begin + step;
		ret = meta->raw_set(Bytes(back_seq_keys[i]),
				Bytes((char *)&back_seqs[i].end, sizeof(uint64_t)));
		if (ret < 0) {
			log_warn("save back seq failed.");
		}
	}
}

uint64_t QueueSeqAlloc::alloc(const Bytes &name, SeqRange::SRType dir) {
	uint32_t code = ssdb::str_hash(name.data(), name.size());
	uint32_t i = code >> (32-QUEUE_SEQ_CON_DEGREE);

	Mutex &mutex = (dir==SeqRange::FRONT) ?
				mutexs[i] : mutexs[QUEUE_SEQ_CONCURRENT+i];
	Locking l(&mutex);

	SeqRange &range = (dir==SeqRange::FRONT) ?
				front_seqs[i] : back_seqs[i];
	assert (range.type == dir);

	// lack of seq, prealloc a range
	if (range.empty()) {
		range.begin = range.end;

		switch (dir) {
			case SeqRange::FRONT:
				range.end -= step;
				break;
			case SeqRange::BACK:
				range.end += step;
				break;
			default:
				assert(0);
		}

		const std::string &key = (dir==SeqRange::FRONT) ?
					front_seq_keys[i] : back_seq_keys[i];
		int ret = meta->raw_set(Bytes(key.data(), key.size()),
					Bytes((char *)&range.end, sizeof(uint64_t)));
		if (ret < 0) {
			log_warn("save queue seq failed.");
		}
	}

	uint64_t seq = range.alloc();
	return seq;
}

static QIterator *qiterator(
		SSDBImpl *ssdb,
		const Bytes &key,
		uint64_t seq_start,
		uint64_t limit,
		uint64_t version,
		Iterator::Direction direction) {
		if(direction == Iterator::FORWARD) {
			std::string start = encode_qitem_key(key, seq_start, version);
			std::string end = encode_qitem_key(key, QITEM_MAX_SEQ, version);
			return new QIterator(ssdb->iterator(start, end, limit), key);
		} else {
			std::string start = encode_qitem_key(key, seq_start, version);
			std::string end = encode_qitem_key(key, QITEM_MIN_SEQ, version);
			return new QIterator(ssdb->rev_iterator(start, end, limit), key);
		}
}

QIterator *SSDBImpl::qscan(const Bytes &key, uint64_t seq_start, uint64_t limit, uint64_t version) {
	return qiterator(this, key, seq_start, limit, version, Iterator::FORWARD);
}
