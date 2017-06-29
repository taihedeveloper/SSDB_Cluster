/*
 * version.h
 *
 * Distributed under terms of the MIT license.
 */

#ifndef SSDB_VERSION_H
#define SSDB_VERSION_H

#include "../include.h"

#define SSDB_VERSION_KEY_PREFIX "\xff|VERSION|"
#define SSDB_VERSION_GLOBAL     "\xff|GLOBAL_VERSION"

#define SSDB_DEPRECATED_KEY_PREFIX "\xff|DEPRECATED|"

inline static
std::string encode_version_key(const Bytes &key) {
	std::string buf;
	buf.append(SSDB_VERSION_KEY_PREFIX, sizeof(SSDB_VERSION_KEY_PREFIX));
	buf.append(key.data(), key.size());

	/* fake version */
	uint64_t version = 0;
	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	int16_t slot = KEY_HASH_SLOT(key);
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
int decode_version_key(const Bytes &slice, std::string *key) {
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(sizeof(SSDB_VERSION_KEY_PREFIX)) == -1) {
		return -1;
	}
	if(decoder.read_data(key, -sizeof(int16_t)-sizeof(uint64_t)) == -1) {
		return -1;
	}
	return 0;
}

inline static
std::string global_version_key() {
	std::string buf(SSDB_VERSION_GLOBAL);

	/* fake version */
	uint64_t version = 0;
	version = big_endian(version);
	buf.append((char*)&version, sizeof(version));

	/* reserve slot */
	int64_t slot = -1;
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
std::string encode_version(char t, uint64_t version) {
	std::string buf;
	buf.append(1, t);
	buf.append((char*)&version, sizeof(version));
	return buf;
}

inline static
int decode_version(const Bytes &slice, char *t, uint64_t *version) {
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1) {
		return -1;
	}
	*t = *slice.data();
	if(decoder.read_uint64(version) == -1) {
		return -1;
	}
	return 0;
}

/* prefix|type|key|version|slot */
inline static
std::string encode_deprecated_key(const Bytes &key, char t, uint64_t version) {
	std::string buf;
	buf.append(SSDB_DEPRECATED_KEY_PREFIX, sizeof(SSDB_DEPRECATED_KEY_PREFIX));
	buf.append(1, t);
	buf.append(key.data(), key.size());
	buf.append((char*)&version, sizeof(version));

	/* reserve slot */
	int16_t slot = -1;
	buf.append((char*)&slot, sizeof(slot));
	return buf;
}

inline static
int decode_deprecated_key(const Bytes &slice, std::string *key, char *t, uint64_t *version) {
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(sizeof(SSDB_DEPRECATED_KEY_PREFIX)+1) == -1) {
		return -1;
	}
	*t = *(slice.data()+sizeof(SSDB_DEPRECATED_KEY_PREFIX));
	if(decoder.read_data(key, -sizeof(int16_t)-sizeof(uint64_t)) == -1) {
		return -1;
	}
	if(decoder.read_uint64(version) == -1) {
		return -1;
	}
	*version = big_endian(*version);
	return 0;
}

#endif /* !VERSION_H */
