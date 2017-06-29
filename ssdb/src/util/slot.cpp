#ifndef UTIL_SLOT_H
#define UTIL_SLOT_H

#include "slot.h"
#include "crc16.h"

uint16_t key_hash_slot(const char *key, size_t key_length, int slot_num) {
	int s, e;
	for(s = 0; s < key_length; s++) {
		if(key[s] == '{') {
			break;
		}
	}
	if(s == key_length) {
		return crc16(key, key_length) % slot_num;
	}

	for(e = s+1; e < key_length; e++) {
		if(key[e] == '}') {
			break;
		}
	}
	if(e == key_length || e == s+1) {
		return crc16(key, key_length) % slot_num;
	}
	return crc16(key+s+1, e-s-1) % slot_num;
}

#endif
