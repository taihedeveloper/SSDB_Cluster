/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <pthread.h>
#include <time.h>
#include "../include.h"
#include "../util/log.h"
#include "../util/hash.h"
#include "ttl.h"

#define EXPIRATION_LIST_KEY "\xff\xff\xff\xff\xff|EXPIRE_LIST|KV"
#define BATCH_SIZE    100

ExpirationHandler::ExpirationHandler(SSDB *ssdb, SSDB_BinLog *binlog){
	this->ssdb = ssdb;
	this->binlog = binlog;
	this->thread_quit = false;

	for (int i = 0; i < EXPIR_CONCURRENT; i++) {
		this->list_name[i] = std::string(EXPIRATION_LIST_KEY) + str(i);
		this->first_timeout[i] = INT64_MAX;
	}
	this->tid = 0;
}

ExpirationHandler::~ExpirationHandler(){
	if (tid != 0) {
		this->stop();
	}
	ssdb = NULL;
}

void ExpirationHandler::start(){
	if(tid != 0) {
		log_warn("expiration thread is running, can't start another.");
		return;
	}
	thread_quit = false;
	int err = pthread_create(&tid, NULL, &ExpirationHandler::thread_func, this);
	if(err != 0){
		log_error("can't create thread: %s", strerror(err));
		tid = 0;
	}
	log_info("expiration thread start tid: %lu.", tid);
}

void ExpirationHandler::stop(){
	if (tid != 0) {
		thread_quit = true;
		pthread_join(tid, NULL);
		log_info("expiration thread stop tid: %lu.", tid);
		tid = 0;
	} else {
		log_warn("expiration thread is not running, can't stop any");
	}
}

int ExpirationHandler::running() {
	return tid != 0 ? 1 : 0;
}

int ExpirationHandler::set_ttl(const Bytes &key, int64_t ttl){
	uint32_t idx = string_hash(key) >> (32-EXPIR_CON_DEGREE);
	Locking l(&mutexs[idx]);

	int64_t expired = time_ms() + ttl * 1000;
	char data[30];
	int size = snprintf(data, sizeof(data), "%" PRId64, expired);
	if(size <= 0){
		log_error("snprintf return error!");
		return -1;
	}

	int ret = 0;
	{
		// no row lock, pervent from dead lock
		Transaction trans(ssdb, Bytes());
		ret = ssdb->zset(this->list_name[idx], key, Bytes(data, size), trans, 0);
	}
	if(ret == -1){
		return -1;
	}

	if(expired < first_timeout[idx]){
		first_timeout[idx] = expired;
	}

	std::string s_key = key.String();
	if(!fast_keys[idx].empty() && expired <= fast_keys[idx].max_score()){
		fast_keys[idx].add(s_key, expired);
		if(fast_keys[idx].size() > BATCH_SIZE){
			log_debug("pop_back");
			fast_keys[idx].pop_back();
		}
	}else{
		fast_keys[idx].del(s_key);
		//log_debug("don't put in fast_keys");
	}

	return 0;
}

int ExpirationHandler::del_ttl(const Bytes &key){
	uint32_t i = string_hash(key) >> (32-EXPIR_CON_DEGREE);
	Locking l(&mutexs[i]);

	if(!this->fast_keys[i].empty()){
		fast_keys[i].del(key.String());
	}
	
	Transaction trans(ssdb, Bytes());
	ssdb->zdel(this->list_name[i], key, trans, 0);

	return 0;
}

int64_t ExpirationHandler::get_ttl(const Bytes &key, const leveldb::Snapshot *snapshot){
	std::string score;
	uint32_t idx = string_hash(key) >> (32-EXPIR_CON_DEGREE);
	if(ssdb->zget(this->list_name[idx], key, &score, 0, snapshot) == 1){
		int64_t ex = str_to_int64(score);
		return (ex - time_ms())/1000;
	}
	return -1;
}

void ExpirationHandler::load_expiration_keys_from_db(int idx, int num){
	ZIterator *it;
	it = ssdb->zscan(this->list_name[idx], "", "", "", num, 0);
	int n = 0;
	while(it->next()){
		n ++;
		std::string &key = it->field;
		int64_t score = str_to_int64(it->score);
		if(score < 2000000000){
			// older version compatible
			score *= 1000;
		}
		fast_keys[idx].add(key, score);
	}
	delete it;
	log_debug("load %d keys into fast_keys", n);
}

void ExpirationHandler::expire_loop(){

	// for each partition
	for (int i = 0; i < EXPIR_CONCURRENT; i++) {

		Locking l(&this->mutexs[i]);
		if(!this->ssdb){
			return;
		}

		if(this->fast_keys[i].empty()){
			this->load_expiration_keys_from_db(i, BATCH_SIZE);
			if(this->fast_keys[i].empty()){
				this->first_timeout[i] = INT64_MAX;

				continue;
			}
		}

		int64_t now = time_ms();
		int64_t score;
		std::string key;
		int max = 5; // prevent from block set_ttl && del_ttl too long
		while (max-- > 0 && this->fast_keys[i].front(&key, &score)) {
			this->first_timeout[i] = score;

			if(score <= now){
				log_debug("expired %s", key.c_str());
				{
					// no row lock, prevent from dead lock
					Transaction trans(ssdb, Bytes());
					ssdb->del(key, trans);
					if (binlog) {
						binlog->write(BinlogType::SYNC, BinlogCommand::K_DEL, key);
					}
				}
				{
					// no row lock, prevent from dead lock
					Transaction trans(ssdb, Bytes());
					ssdb->zdel(this->list_name[i], key, trans, 0);
					this->fast_keys[i].pop_front();
				}

			} else {
				break;
			}
		}

	}/* end of for */
}

void* ExpirationHandler::thread_func(void *arg){
	SET_PROC_NAME("expiration_handler");
	ExpirationHandler *handler = (ExpirationHandler *)arg;

	while(!handler->thread_quit){
		bool immediately = false;
		int64_t now = time_ms();
		for (int i = 0; i < EXPIR_CONCURRENT; i++) {
			if (handler->first_timeout[i] <= now) {
				immediately = true;
				break;
			}
		}

		if(!immediately){
			usleep(10 * 1000);
			continue;
		}

		handler->expire_loop();
	}

	log_debug("ExpirationHandler thread quit");
	return (void *)NULL;
}

uint32_t ExpirationHandler::string_hash(const Bytes &s) {
	return ssdb::str_hash(s.data(), s.size());
}

uint64_t ExpirationHandler::expires() {
	uint64_t nexpires = 0;
	for(int i = 0; i < EXPIR_CONCURRENT; i++) {
		nexpires += ssdb->zsize(this->list_name[i], 0);
	}
	return nexpires;
}
