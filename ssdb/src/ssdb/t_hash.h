/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_HASH_H_
#define SSDB_HASH_H_

#include "../include.h"
#include "ssdb_impl.h"

/* compatible encode/decode */
inline static
std::string encode_hash_key_ex(const Bytes &key, const Bytes &field) {
	std::string buf;
	buf.append(1, DataType::HASH);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	buf.append(1, '=');
	buf.append(field.data(), field.size());

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
int decode_hash_key_ex(const Bytes &slice, std::string *key, std::string *field) {
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_8_data(key) == -1){
		return -1;
	}
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(field, -sizeof(int16_t)) == -1){
		return -1;
	}
	return 0;
}

/* end of compatible encode/decode */

/* type(1)|key|version|slot(int16_t) */
inline static
std::string encode_hsize_key(const Bytes &key, uint64_t version){
	std::string buf;
	buf.append(1, DataType::HSIZE);
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
int decode_hsize_key(const Bytes &slice, std::string *key, uint64_t *version){
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

/* type(1)|len(key)|key|version|=(1)|field|slot(int16_t) */
inline static
std::string encode_hash_key(const Bytes &key, const Bytes &field, uint64_t version){
	std::string buf;
	buf.append(1, DataType::HASH);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	buf.append(1, '=');
	buf.append(field.data(), field.size());

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
int decode_hash_key(const Bytes &slice, std::string *key, std::string *field, uint64_t *version){
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
	if(decoder.read_data(field, -sizeof(int16_t)) == -1){
		return -1;
	}
	return 0;
}

inline static
std::string hash_key_prefix(const Bytes &key, uint64_t version) {
	std::string buf;
	buf.append(1, DataType::HASH);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	buf.append(1, '=');

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
std::string hash_key_raw_prefix(const Bytes &slice, uint64_t *version) {
	std::string key, field;
	if(decode_hash_key(slice, &key, &field, version) == -1) {
		return "";
	}
	return hash_key_prefix(key, *version);
}

#endif
