/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_FILE_H_
#define UTIL_FILE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>

static inline
bool file_exists(const std::string &filename){
	struct stat st;
	return stat(filename.c_str(), &st) == 0;
}

static inline
bool is_dir(const std::string &filename){
	struct stat st;
	if(stat(filename.c_str(), &st) == -1){
		return false;
	}
	return (bool)S_ISDIR(st.st_mode);
}

static inline
bool is_file(const std::string &filename){
	struct stat st;
	if(stat(filename.c_str(), &st) == -1){
		return false;
	}
	return (bool)S_ISREG(st.st_mode);
}

static inline
time_t last_modify_time(const std::string &filename) {
	struct stat st;
	if (stat(filename.c_str(), &st) != 0) {
		return 0;
	}
	return st.st_mtime;
}

static inline
int create_file(const std::string &filename) {
	int fd = open(filename.c_str(), O_CREAT|O_EXCL);
	if (fd < 0) {
		return fd;
	}
	close(fd);
	return 0;
}

static inline
uint64_t filesize(int fd) {
	assert (fd >= 0);

	struct stat st;
	int ret = fstat(fd, &st);
	if (ret != 0) {
		return 0;
	}
	return st.st_size;
}

// return number of bytes read
static inline
int file_get_contents(const std::string &filename, std::string *content){
	char buf[8192];
	FILE *fp = fopen(filename.c_str(), "rb");
	if(!fp){
		return -1;
	}
	int ret = 0;
	while(!feof(fp) && !ferror(fp)){
		int n = fread(buf, 1, sizeof(buf), fp);
		if(n > 0){
			ret += n;
			content->append(buf, n);
		}
	}
	fclose(fp);
	return ret;
}

// return number of bytes written
static inline
int file_put_contents(const std::string &filename, const std::string &content){
	FILE *fp = fopen(filename.c_str(), "wb");
	if(!fp){
		return -1;
	}
	int ret = fwrite(content.data(), 1, content.size(), fp);
	fclose(fp);
	return ret == (int)content.size()? ret : -1;
}
/*
int remove_file(const std::string &filename) {
	return unlink(filename.c_str());
}*/

#endif
