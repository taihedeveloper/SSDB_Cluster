#ifndef SSDB_COMPARATOR_H_
#define SSDB_COMPARATOR_H_

#include "leveldb/slice.h"
#include "leveldb/comparator.h"

class SlotBytewiseComparatorImpl : public leveldb::Comparator {
public:
	virtual const char *Name() const;

	virtual int Compare(const leveldb::Slice &a, const leveldb::Slice &b) const;

	virtual void FindShortestSeparator(std::string *start, const leveldb::Slice &limit) const;

	virtual void FindShortSuccessor(std::string *key) const;

	static leveldb::Comparator *getComparator();
private:
	SlotBytewiseComparatorImpl(){};
	static leveldb::Comparator *ins;
};

#endif
