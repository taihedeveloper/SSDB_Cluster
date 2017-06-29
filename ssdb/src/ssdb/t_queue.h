/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_QUEUE_H_
#define SSDB_QUEUE_H_

#include "../include.h"
#include "ssdb_impl.h"
#include "../util/thread.h"

const uint64_t QFRONT_SEQ = 2;
const uint64_t QBACK_SEQ  = 3;
const uint64_t QITEM_MIN_SEQ = 10000;
const uint64_t QITEM_MAX_SEQ = 9223372036854775807ULL;
const uint64_t QITEM_SEQ_INIT = QITEM_MAX_SEQ/2;

// [begin, end) for back seq, (end, begin] for front seq.
struct SeqRange {
	enum SRType {
		FRONT = 1,
		BACK = 2,
	} type;
	uint64_t begin;
	uint64_t end;

	bool valid() const { return type==FRONT ? end <= begin : end >= begin; }
	bool empty() const { return begin == end; }
	uint64_t alloc() { return type==FRONT ? begin-- : begin++; }
};


#define QUEUE_SEQ_CON_DEGREE 4
#define QUEUE_SEQ_CONCURRENT (1<<QUEUE_SEQ_CON_DEGREE)
#define QUEUE_SAFE_INCREMENT 100000000
#define QUEUE_FRONT_SEQ "\xff\xff\xff\xff\xff|QUEUE_FRONT_SEQ"
#define QUEUE_BACK_SEQ "\xff\xff\xff\xff\xff|QUEUE_BACK_SEQ"

class QueueSeqAlloc {
private:
	SSDB *meta;
	uint64_t step;

	std::string front_seq_keys[QUEUE_SEQ_CONCURRENT];
	std::string back_seq_keys[QUEUE_SEQ_CONCURRENT];
	SeqRange front_seqs[QUEUE_SEQ_CONCURRENT];
	SeqRange back_seqs[QUEUE_SEQ_CONCURRENT];
	Mutex mutexs[QUEUE_SEQ_CONCURRENT*2];

public:
	QueueSeqAlloc(SSDB *meta, int16_t slotcount, uint64_t step=1000000);
	~QueueSeqAlloc() { };

	uint64_t alloc(const Bytes &queuename, SeqRange::SRType direction);
};

/* compatible encode/decode */

inline static
std::string encode_qitem_key_ex(const Bytes &key, uint64_t seq) {
	std::string buf;
	buf.append(1, DataType::QUEUE);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	seq = big_endian(seq);
	buf.append((char *)&seq, sizeof(uint64_t));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
int decode_qitem_key_ex(const Bytes &slice, std::string *key, uint64_t *seq) {
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_8_data(key) == -1){
		return -1;
	}
	if(decoder.read_uint64(seq) == -1){
		return -1;
	}
	*seq = big_endian(*seq);
	return 0;
}

/* end of compatible encode/decode */

inline static
std::string encode_qsize_key(const Bytes &key, uint64_t version){
	std::string buf;
	buf.append(1, DataType::QSIZE);
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
int decode_qsize_key(const Bytes &slice, std::string *key, uint64_t *version){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(key, -sizeof(int16_t)-sizeof(uint64_t)) == -1){
		return -1;
	}
	if(decoder.read_uint64(version) == -1) {
		return -1;
	}
	*version = big_endian(*version);
	return 0;
}

/* type(1)|len(key)|key|version|seq(uint64_t)|slot(int16_t)*/
inline static
std::string encode_qitem_key(const Bytes &key, uint64_t seq, uint64_t version){
	std::string buf;
	buf.append(1, DataType::QUEUE);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	seq = big_endian(seq);
	buf.append((char *)&seq, sizeof(uint64_t));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
int decode_qitem_key(const Bytes &slice, std::string *key, uint64_t *seq, uint64_t *version){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_8_data(key) == -1){
		return -1;
	}
	if(decoder.read_uint64(version) == -1) {
		return -1;
	}
	*version = big_endian(*version);
	if(decoder.read_uint64(seq) == -1){
		return -1;
	}
	*seq = big_endian(*seq);
	return 0;
}

inline static
std::string qitem_key_prefix(const Bytes &key, uint64_t version) {
	std::string buf;
	buf.append(1, DataType::QUEUE);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
std::string qitem_key_raw_prefix(const Bytes &slice, uint64_t *version) {
	std::string key;
	uint64_t seq;
	if(decode_qitem_key(slice, &key, &seq, version) == -1) {
		return "";
	}
	return qitem_key_prefix(key, *version);
}

#endif
