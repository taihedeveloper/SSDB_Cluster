/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "options.h"
#include "../util/strings.h"

Options::Options(){
	Config c;
	this->load(c);
}

void Options::load(const Config &conf){
	cache_size = (size_t)conf.get_num("leveldb.cache_size");
	max_open_files = (size_t)conf.get_num("leveldb.max_open_files");
	write_buffer_size = (size_t)conf.get_num("leveldb.write_buffer_size");
	block_size = (size_t)conf.get_num("leveldb.block_size");
	compaction_speed = conf.get_num("leveldb.compaction_speed");
	compression = conf.get_str("leveldb.compression");
	//int binlog = conf.get_num("rpl.binlog");
	binlog_dir = conf.get_str("rpl.binlog_dir");
	int sync_binlog = conf.get_num("rpl.sync_binlog");
	max_binlog_size = conf.get_num("rpl.max_binlog_size");
	std::string purge_logs_span_str = conf.get_str("rpl.purge_logs_span");

	strtolower(&compression);
	if(compression != "no"){
		compression = "yes";
	}

	//this->binlog = (binlog==1) ? true : false;
	// always enable binlog
	this->binlog = true;
	this->sync_binlog = (sync_binlog==1) ? true : false;

	if(cache_size <= 0){
		cache_size = 8;
	}
	if(write_buffer_size <= 0){
		write_buffer_size = 4;
	}
	if(block_size <= 0){
		block_size = 4;
	}
	if(max_open_files <= 0){
		max_open_files = cache_size / 1024 * 300;
		if(max_open_files < 500){
			max_open_files = 500;
		}
		if(max_open_files > 1000){
			max_open_files = 1000;
		}
	}

	purge_logs_span = str_to_span(purge_logs_span_str);
	if (purge_logs_span < 0) {
		purge_logs_span = 0;
	}
}
