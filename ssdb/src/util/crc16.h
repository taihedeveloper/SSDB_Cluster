#ifndef UTIL_CRC16_H
#define UTIL_CRC16_H

#include <stdio.h>
#include <stdint.h>

uint16_t crc16(const char *key, size_t key_length);

#endif
