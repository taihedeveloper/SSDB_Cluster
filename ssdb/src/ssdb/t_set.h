/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_SET_H_
#define SSDB_SET_H_

#include "../include.h"
#include "ssdb_impl.h"

/* compatible encode/decode */
static inline
std::string encode_set_key_ex(const Bytes &key, const Bytes &elem) {
	std::string buf;
	buf.append(1, DataType::SET);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());
	buf.append(elem.data(), elem.size());

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

static inline
int decode_set_key_ex(const Bytes &slice, std::string *key, std::string *elem) {
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1) {
		return -1;
	}
	if(decoder.read_8_data(key) == -1) {
		return -1;
	}
	if(decoder.read_data(elem, -sizeof(int16_t)) == -1) {
		return -1;
	}
	return 0;
}

/* end of compatible encode/decode */

/* type(1)|key|version(uint64_t)|slot(int16_t) */
static inline
std::string encode_ssize_key(const Bytes &key, uint64_t version) {
	std::string buf;
	buf.append(1, DataType::SSIZE);
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

static inline
int decode_ssize_key(const Bytes &slice, std::string *key, uint64_t *version) {
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

/* type(1)|len(key)|key|version|elem|slot */
static inline
std::string encode_set_key(const Bytes &key, const Bytes &elem, uint64_t version) {
	std::string buf;
	buf.append(1, DataType::SET);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	buf.append(elem.data(), elem.size());

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

static inline
int decode_set_key(const Bytes &slice, std::string *key, std::string *elem, uint64_t *version) {
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1) {
		return -1;
	}
	if(decoder.read_8_data(key) == -1) {
		return -1;
	}
	if(decoder.read_uint64(version) == -1) {
		return -1;
	}
	*version = big_endian(*version);
	if(decoder.read_data(elem, -sizeof(int16_t)) == -1) {
		return -1;
	}
	return 0;
}

static inline
std::string set_key_prefix(const Bytes &key, uint64_t version) {
	std::string buf;
	buf.append(1, DataType::SET);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

static inline
std::string set_key_raw_prefix(const Bytes &slice, uint64_t *version) {
	std::string key, field;
	if(decode_set_key(slice, &key, &field, version) != 0) {
		return "";
	}
	return set_key_prefix(key, *version);
}

#endif
