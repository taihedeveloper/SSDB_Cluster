/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/

#include "transaction.h"
#include "ssdb.h"

Transaction::Transaction(SSDB *db_, const std::string &key)
	: db(db_), lock_key(key) {
	db->lock_key(lock_key);
}

Transaction::Transaction(SSDB *db_, const Bytes &key) 
	: db(db_), lock_key(key.data(), key.size()) {
	if (!lock_key.empty()) {
		db->lock_key(lock_key);	
	}
}

Transaction::~Transaction() {
	if (!lock_key.empty()) {
		db->unlock_key(lock_key);
	}
}

void Transaction::begin() {
	updates.Clear();
}

void Transaction::rollback() {
	updates.Clear();
}

Transaction::Status Transaction::commit() {
	WriteOptions option;
	return db->write(option, &updates);
}

void Transaction::del(const Bytes &key) {
	updates.Delete(Slice(key.data(), key.size()));
}
void Transaction::put(const Bytes &key, const Bytes &val) {
	updates.Put(Slice(key.data(), key.size()), Slice(val.data(), val.size()));
}

void Transaction::del(const std::string &key) {
	updates.Delete(Slice(key.data(), key.size()));
}

void Transaction::put(const std::string &key, const std::string &val) {
	updates.Put(Slice(key.data(), key.size()), Slice(val.data(), val.size()));
}

