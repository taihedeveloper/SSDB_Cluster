/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_ZSET_H_
#define SSDB_ZSET_H_

#include "../include.h"
#include "ssdb_impl.h"

#define encode_score(s) big_endian((uint64_t)(s))
#define decode_score(s) big_endian((uint64_t)(s))

/* compatible encode/decode */
static inline
std::string encode_zset_key_ex(const Bytes &key, const Bytes &field) {
	std::string buf;
	buf.append(1, DataType::ZSET);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	buf.append(1, (uint8_t)field.size());
	buf.append(field.data(), field.size());

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

static inline
int decode_zset_key_ex(const Bytes &slice, std::string *key, std::string *field) {
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_8_data(key) == -1){
		return -1;
	}
	if(decoder.read_8_data(field) == -1){
		return -1;
	}
	return 0;
}

/* end of compatible encode/decode */

/* type|key|version|slot */
static inline
std::string encode_zsize_key(const Bytes &key, uint64_t version){
	std::string buf;
	buf.append(1, DataType::ZSIZE);
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
int decode_zsize_key(const Bytes &slice, std::string *key, uint64_t *version){
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

/* type|len(key)|key|version|len(field)|field|slot */
static inline
std::string encode_zset_key(const Bytes &key, const Bytes &field, uint64_t version){
	std::string buf;
	buf.append(1, DataType::ZSET);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	buf.append(1, (uint8_t)field.size());
	buf.append(field.data(), field.size());

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

static inline
int decode_zset_key(const Bytes &slice, std::string *key, std::string *field, uint64_t *version){
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
	if(decoder.read_8_data(field) == -1){
		return -1;
	}
	return 0;
}

/* type(1)|len(key)|key|version|score|=|field|slot(int16_t) */
static inline
std::string encode_zscore_key(const Bytes &key, const Bytes &field, const Bytes &score, uint64_t version){
	std::string buf;
	buf.append(1, DataType::ZSCORE);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	int64_t s = score.Int64();
	if(s < 0){
		buf.append(1, '-');
	}else{
		buf.append(1, '=');
	}
	s = encode_score(s);

	buf.append((char *)&s, sizeof(uint64_t));
	buf.append(1, '=');
	buf.append(field.data(), field.size());

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

static inline
int decode_zscore_key(const Bytes &slice, std::string *key, std::string *field, std::string *score, uint64_t *version){
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
	if(decoder.skip(1) == -1){
		return -1;
	}
	int64_t s;
	if(decoder.read_int64(&s) == -1){
		return -1;
	}else{
		if(score != NULL){
			s = decode_score(s);
			score->assign(str(s));
		}
	}
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(field, -sizeof(int16_t)) == -1){
		return -1;
	}
	return 0;
}

static inline
std::string zset_key_prefix(const Bytes &key, uint64_t version) {
	std::string buf;
	buf.append(1, DataType::ZSET);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

static inline
std::string zset_key_raw_prefix(const Bytes &slice, uint64_t *version) {
	std::string key, field;
	if(decode_zset_key(slice, &key, &field, version) != 0) {
		return "";
	}
	return zset_key_prefix(key, *version);
}

#endif
