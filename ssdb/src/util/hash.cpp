#include <string.h>
#include "hash.h"

namespace ssdb {

uint32_t Hash(const char* data, size_t n, uint32_t seed) {
  // Similar to murmur hash
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* end = data + n;
  uint32_t h = seed ^ (n * m);

  while (data < end) {
	h += static_cast<unsigned char>(data[0]);
	h *= m;
	h ^= (h >> r);
	data ++;
  }

  return h;
}

uint32_t str_hash(const char *data, size_t n) {
	return ssdb::Hash(data, n, 0xbc9f1d34);
}

}  // namespace ssdb
