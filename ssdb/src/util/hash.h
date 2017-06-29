#ifndef STORAGE_SSDB_UTIL_HASH_H_
#define STORAGE_SSDB_UTIL_HASH_H_

#include <stddef.h>
#include <stdint.h>

namespace ssdb {

extern uint32_t Hash(const char* data, size_t n, uint32_t seed);

extern uint32_t str_hash(const char *data, size_t n);

}

#endif  // STORAGE_SSDB_UTIL_HASH_H_
