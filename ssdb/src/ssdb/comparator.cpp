#include <stdint.h>
#include "comparator.h"

const char *SlotBytewiseComparatorImpl::Name() const {
	return "ssdb.SlotBytewiseComparatorImpl";
}

int SlotBytewiseComparatorImpl::Compare(const leveldb::Slice &a, const leveldb::Slice &b) const {
	int16_t slota = *reinterpret_cast<const int16_t*>(a.data()+a.size()-sizeof(int16_t));
	int16_t slotb = *reinterpret_cast<const int16_t*>(b.data()+b.size()-sizeof(int16_t));
	if(slota < slotb) {
		return -1;
	} else if(slota > slotb) {
		return 1;
	}
	leveldb::Slice aa = leveldb::Slice(a.data(), a.size()-sizeof(int16_t));
	leveldb::Slice bb = leveldb::Slice(b.data(), b.size()-sizeof(int16_t));
	return leveldb::BytewiseComparator()->Compare(aa, bb);
}

void SlotBytewiseComparatorImpl::FindShortestSeparator(std::string *start, const leveldb::Slice &limit) const {
	//return leveldb::BytewiseComparator()->FindShortestSeparator(start, limit);
}

void SlotBytewiseComparatorImpl::FindShortSuccessor(std::string *key) const {
	//return leveldb::BytewiseComparator()->FindShortSuccessor(key);
}

leveldb::Comparator *SlotBytewiseComparatorImpl::getComparator() {
	if (ins == NULL) {
		ins = new SlotBytewiseComparatorImpl();
	}
	return ins;
}
    
leveldb::Comparator *SlotBytewiseComparatorImpl::ins = NULL;
