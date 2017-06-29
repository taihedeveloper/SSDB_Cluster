
inline int bitmapTestBit(unsigned char *bitmap, int pos) {
	off_t byte = pos/8;
	int bit = pos&7;
	return (bitmap[byte] & (1<<bit)) != 0;
}

inline void bitmapSetBit(unsigned char *bitmap, int pos) {
	off_t byte = pos/8;
	int bit = pos&7;
	bitmap[byte] |= 1<<bit;
}

inline void bitmapClearBit(unsigned char *bitmap, int pos) {
	off_t byte = pos/8;
	int bit = pos&7;
	bitmap[byte] &= ~(1<<bit);
}
