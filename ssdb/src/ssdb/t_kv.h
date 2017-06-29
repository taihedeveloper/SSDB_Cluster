/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_KV_H_
#define SSDB_KV_H_

#include "../include.h"
#include "ssdb_impl.h"

/* compatible encode/decode */

static inline
std::string encode_kv_key_ex(const Bytes &key) {
	std::string buf;
	buf.append(1, DataType::KV);
	buf.append(key.data(), key.size());

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

static inline
int decode_kv_key_ex(const Bytes &slice, std::string *key) {
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1) {
		return -1;
	}
	if(decoder.read_data(key, -sizeof(int16_t)) == -1) {
		return -1;
	}
	return 0;
}

/* end of compatible encode/decode */

/* type(1)|key|version(uint64_t)|slot(int16_t)*/
static inline
std::string encode_kv_key(const Bytes &key, uint64_t version){
	std::string buf;
	buf.append(1, DataType::KV);
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));

	return buf;
}

static inline
int decode_kv_key(const Bytes &slice, std::string *key, uint64_t *version) {
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1) {
		return -1;
	}
	if(decoder.read_data(key, -sizeof(int16_t)-sizeof(uint64_t)) == -1) {
		return -1;
	}
	if(decoder.read_uint64(version) == -1) {
		return -1;
	}
	*version = big_endian(*version);
	return 0;
}

static inline
std::string kv_key_prefix(const Bytes &key, uint64_t version) {
	return encode_kv_key(key, version);
}

static inline
std::string kv_key_raw_prefix(const Bytes &slice, uint64_t *version) {
	std::string key;
	if (decode_kv_key(slice, &key, version) == -1) {
		return "";
	}
	return kv_key_prefix(key, *version);
}

#endif
