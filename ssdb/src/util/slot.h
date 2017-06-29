#ifndef UTIL_SLOT_H
#define UTIL_SLOT_H

#include <stdio.h>
#include <stdint.h>

uint16_t key_hash_slot(const char *key, size_t key_length, int slot_num);

#endif
