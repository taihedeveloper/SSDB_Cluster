/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_ITERATOR_H_
#define SSDB_ITERATOR_H_

#include <inttypes.h>
#include <string>
#include "../util/bytes.h"

namespace leveldb{
	class Iterator;
	class Comparator;
}

class Iterator{
public:
	enum Direction{
		FORWARD, BACKWARD
	};
	Iterator(leveldb::Iterator *it,
			const std::string &end,
			uint64_t limit,
			Direction direction=Iterator::FORWARD);
	~Iterator();
	bool skip(uint64_t offset);
	bool next();
	Bytes key();
	Bytes val();

private:
	leveldb::Iterator *it;
	leveldb::Comparator *cmp;
	std::string end;
	uint64_t limit;
	bool is_first;
	int direction;
};


class KIterator{
public:
	std::string key;
	std::string val;
	uint64_t version;

	KIterator(Iterator *it);
	~KIterator();
	void return_val(bool onoff);
	bool next();
private:
	Iterator *it;
	bool return_val_;
};


class HIterator{
public:
	std::string key;
	std::string field;
	std::string val;
	uint64_t version;

	HIterator(Iterator *it, const Bytes &key);
	~HIterator();
	void return_val(bool onoff);
	bool next();
private:
	Iterator *it;
	bool return_val_;
};


class ZIterator{
public:
	std::string key;
	std::string field;
	std::string score;
	uint64_t version;

	ZIterator(Iterator *it, const Bytes &key);
	~ZIterator();
	bool skip(uint64_t offset);
	bool next();
private:
	Iterator *it;
};


class SIterator {
public:
	std::string key;
	std::string elem;
	uint64_t version;

	SIterator(Iterator *it, const Bytes &key);
	~SIterator();
	bool skip(uint64_t offset);
	bool next();
private:
	Iterator *it;
};

class QIterator {
public:
	std::string key;
	std::string elem;
	std::string val;
	uint64_t seq;
	uint64_t version;
	QIterator(Iterator *it, const Bytes &key);
	~QIterator();
	bool skip(uint64_t offset);
	bool next();
private:
	Iterator *it;
};

#endif
