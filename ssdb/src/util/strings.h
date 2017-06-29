/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_STRING_H
#define UTIL_STRING_H

#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string>
#include <algorithm>


inline static
int is_empty_str(const char *str){
	const char *p = str;
	while(*p && isspace(*p)){
		p++;
	}
	return *p == '\0';
}

/* 返回左边不包含空白字符的字符串的指针 */
inline static
char *ltrim(const char *str){
	const char *p = str;
	while(*p && isspace(*p)){
		p++;
	}
	return (char *)p;
}

/* 返回指向字符串结尾的指针, 会修改字符串内容 */
inline static
char *rtrim(char *str){
	char *p;
	p = str + strlen(str) - 1;
	while(p >= str && isspace(*p)){
		p--;
	}
	*(++p) = '\0';
	return p;
}

/* 返回左边不包含空白字符的字符串的指针 */
inline static
char *trim(char *str){
	char *p;
	p = ltrim(str);
	rtrim(p);
	return p;
}

inline static
void strtolower(std::string *str){
	std::transform(str->begin(), str->end(), str->begin(), ::tolower);
}

inline static
void strtoupper(std::string *str){
	std::transform(str->begin(), str->end(), str->begin(), ::toupper);
}

inline static
std::string real_dirname(const char *filepath){
	std::string dir;
	if(filepath[0] != '/'){
		char buf[1024];
		char *p = getcwd(buf, sizeof(buf));
		if(p != NULL){
			dir.append(p);
		}
		dir.append("/");
	}

	const char *p = strrchr(filepath, '/');
	if(p != NULL){
		dir.append(filepath, p - filepath);
	}
	return dir;
}

inline static
std::string str_escape(const char *s, int size){
	static const char *hex = "0123456789abcdef";
	std::string ret;
	for(int i=0; i<size; i++){
		char c = s[i];
		switch(c){
			case '\r':
				ret.append("\\r");
				break;
			case '\n':
				ret.append("\\n");
				break;
			case '\t':
				ret.append("\\t");
				break;
			case '\\':
				ret.append("\\\\");
				break;
			case ' ':
				ret.push_back(c);
				break;
			default:
				if(c >= '!' && c <= '~'){
					ret.push_back(c);
				}else{
					ret.append("\\x");
					unsigned char d = c;
					ret.push_back(hex[d >> 4]);
					ret.push_back(hex[d & 0x0f]);
				}
				break;
		}
	}
	return ret;
}

inline static
std::string str_escape(const std::string &s){
	return str_escape(s.data(), (int)s.size());
}

inline static
int hex_int(char c){
	if(c >= '0' && c <= '9'){
		return c - '0';
	}else{
		return c - 'a' + 10;
	}
}

inline static
std::string str_unescape(const char *s, int size){
	std::string ret;
	for(int i=0; i<size; i++){
		char c = s[i];
		if(c != '\\'){
			ret.push_back(c);
		}else{
			if(i >= size - 1){
				continue;
			}
			char c2 = s[++i];
			switch(c2){
				case 'a':
					ret.push_back('\a');
					break;
				case 'b':
					ret.push_back('\b');
					break;
				case 'f':
					ret.push_back('\f');
					break;
				case 'v':
					ret.push_back('\v');
					break;
				case 'r':
					ret.push_back('\r');
					break;
				case 'n':
					ret.push_back('\n');
					break;
				case 't':
					ret.push_back('\t');
					break;
				case '\\':
					ret.push_back('\\');
					break;
				case 'x':
					if(i < size - 2){
						char c3 = s[++i];
						char c4 = s[++i];
						ret.push_back((char)((hex_int(c3) << 4) + hex_int(c4)));
					}
					break;
				default:
					ret.push_back(c2);
					break;
			}
		}
	}
	return ret;
}

inline static
std::string str_unescape(const std::string &s){
	return str_unescape(s.data(), (int)s.size());
}

inline static
std::string hexmem(const void *p, int size){
	return str_escape((char *)p, size);
	/*
	std::string ret;
	char buf[4];
	for(int i=0; i<size; i++){
		char c = ((char *)p)[i];
		if(isalnum(c) || isprint(c)){
			ret.append(1, c);
		}else{
			switch(c){
				case '\r':
					ret.append("\\r", 2);
					break;
				case '\n':
					ret.append("\\n", 2);
					break;
				default:
					sprintf(buf, "\\%02x", (unsigned char)c);
					ret.append(buf, 3);
			}
		}
	}
	return ret;
	*/
}

// TODO: mem_printf("%5c%d%s", p, size);
static inline
void dump(const void *p, int size, const char *msg = NULL){
	if(msg == NULL){
		printf("dump <");
	}else{
		printf("%s <", msg);
	}
	std::string s = hexmem(p, size);
	printf("%s>\n", s.c_str());
}


static inline
std::string str(const char *s){
	return std::string(s);
}

static inline
std::string str(int v){
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%d", v);
	return std::string(buf);
}

static inline
std::string str(int64_t v){
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%" PRId64 "", v);
	return std::string(buf);
}

static inline
std::string str(uint64_t v){
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%" PRIu64 "", v);
	return std::string(buf);
}

static inline
std::string str(double v){
	char buf[21] = {0};
	if(v - floor(v) == 0){
		snprintf(buf, sizeof(buf), "%.0f", v);
	}else{
		snprintf(buf, sizeof(buf), "%f", v);
	}
	return std::string(buf);
}

static inline
std::string str(float v){
	return str((double)v);
}

// all str_to_xx methods set errno on error

static inline
int str_to_int(const std::string &str){
	const char *start = str.c_str();
	char *end;
	int ret = (int)strtol(start, &end, 10);
	// the WHOLE string must be string represented integer
	if(*end == '\0' && size_t(end - start) == str.size()){
		errno = 0;
	}else{
		// strtoxx do not set errno all the time!
		if(errno == 0){
			errno = EINVAL;
		}
	}
	return ret;
}

static inline
int str_to_int(const char *p, int size){
	return str_to_int(std::string(p, size));
}

static inline
int64_t str_to_int64(const std::string &str){
	const char *start = str.c_str();
	char *end;
	int64_t ret = (int64_t)strtoll(start, &end, 10);
	// the WHOLE string must be string represented integer
	if(*end == '\0' && size_t(end - start) == str.size()){
		errno = 0;
	}else{
		// strtoxx do not set errno all the time!
		if(errno == 0){
			errno = EINVAL;
		}
	}
	return ret;
}

static inline
int64_t str_to_int64(const char *p, int size){
	return str_to_int64(std::string(p, size));
}

static inline
uint64_t str_to_uint64(const std::string &str){
	const char *start = str.c_str();
	char *end;
	uint64_t ret = (uint64_t)strtoull(start, &end, 10);
	// the WHOLE string must be string represented integer
	if(*end == '\0' && size_t(end - start) == str.size()){
		errno = 0;
	}else{
		// strtoxx do not set errno all the time!
		if(errno == 0){
			errno = EINVAL;
		}
	}
	return ret;
}

static inline
uint64_t str_to_uint64(const char *p, int size){
	return str_to_uint64(std::string(p, size));
}

static inline
double str_to_double(const char *p, int size){
	return atof(std::string(p, size).c_str());
}

static inline
std::string substr(const std::string &str, int start, int size){
	if(start < 0){
		start = (int)str.size() + start;
	}
	if(size < 0){
		// 忽略掉 abs(size) 个字节
		size = ((int)str.size() + size) - start;
	}
	if(start < 0 || size_t(start) >= str.size() || size < 0){
		return "";
	}
	return str.substr(start, size);
}

static inline
std::string str_slice(const std::string &str, int start, int end){
	if(start < 0){
		start = (int)str.size() + start;
	}
	int size;
	if(end < 0){
		size = ((int)str.size() + end + 1) - start;
	}else{
		size = end - start + 1;
	}
	if(start < 0 || size_t(start) >= str.size() || size < 0){
		return "";
	}
	return str.substr(start, size);
}

static inline
int bitcount(const char *p, int size){
	int n = 0;
	for(int i=0; i<size; i++){
		unsigned char c = (unsigned char)p[i];
		while(c){
			n += c & 1;
			c = c >> 1;
		}
	}
	return n;
}

static inline
time_t str_to_span(const std::string &str){
	const char *start = str.c_str();
	char *end;
	int ret = (int)strtol(start, &end, 10);
	char t = '\0';
	if(size_t(end - start) == str.size() - 1){
		t = *end;
	}

	switch (t) {
	case 's':
		ret *= 1;
		break;
	case 'm':
		ret *= 60;
		break;
	case 'h':
		ret *= 3600;
		break;
	case 'd':
		ret *= 86400;
		break;
	default:
		ret = 0;
		errno = EINVAL;
		break;
	}

	return ret;
}

static int stringmatchlen(const char *pattern, int patternLen,
		const char *string, int stringLen, int nocase) {
	while(patternLen) {
		switch(pattern[0]) {
		case '*':
			while (pattern[1] == '*') {
				pattern++;
				patternLen--;
			}
			if (patternLen == 1)
				return 1; /* match */
			while(stringLen) {
				if (stringmatchlen(pattern+1, patternLen-1, string, stringLen, nocase))
				return 1; /* match */
				string++;
				stringLen--;
			}
			return 0; /* no match */
			break;
		case '?':
			if (stringLen == 0)
				return 0; /* no match */
			string++;
			stringLen--;
			break;
		case '[':
		{
			int no, match;

			pattern++;
			patternLen--;
			no = pattern[0] == '^';
			if (no) {
				pattern++;
				patternLen--;
			}
			match = 0;
			while(1) {
				if (pattern[0] == '\\') {
					pattern++;
					patternLen--;
					if (pattern[0] == string[0])
						match = 1;
				} else if (pattern[0] == ']') {
					break;
				} else if (patternLen == 0) {
					pattern--;
					patternLen++;
					break;
				} else if (pattern[1] == '-' && patternLen >= 3) {
					int start = pattern[0];
					int end = pattern[2];
					int c = string[0];
					if (start > end) {
					int t = start;
						start = end;
						end = t;
					}
					if (nocase) {
						start = tolower(start);
						end = tolower(end);
						c = tolower(c);
				}
					pattern += 2;
					patternLen -= 2;
					if (c >= start && c <= end)
						match = 1;
				} else {
					if (!nocase) {
						if (pattern[0] == string[0])
							match = 1;
					} else {
						if (tolower((int)pattern[0]) == tolower((int)string[0]))
							match = 1;
					}
				}
				pattern++;
				patternLen--;
			}
			if (no)
				match = !match;
			if (!match)
				return 0; /* no match */
			string++;
			stringLen--;
			break;
		}
		case '\\':
			if (patternLen >= 2) {
				pattern++;
				patternLen--;
			}
			/* fall through */
		default:
			if (!nocase) {
				if (pattern[0] != string[0])
					return 0; /* no match */
			} else {
				if (tolower((int)pattern[0]) != tolower((int)string[0]))
					return 0; /* no match */
			}
			string++;
			stringLen--;
			break;
		}
		pattern++;
		patternLen--;
		if (stringLen == 0) {
			while(*pattern == '*') {
				pattern++;
				patternLen--;
			}
			break;
		}
    }
	if (patternLen == 0 && stringLen == 0)
		return 1;
	return 0;
}

// is big endia. TODO: auto detect
#if 0
	#define big_endian(v) (v)
#else
	static inline
	uint16_t big_endian(uint16_t v){
		return (v>>8) | (v<<8);
	}

	static inline
	uint32_t big_endian(uint32_t v){
		return (v >> 24) | ((v >> 8) & 0xff00) | ((v << 8) & 0xff0000) | (v << 24);
	}

	static inline
	uint64_t big_endian(uint64_t v){
		uint32_t h = v >> 32;
		uint32_t l = v & 0xffffffffull;
		return big_endian(h) | ((uint64_t)big_endian(l) << 32);
	}

#endif

#define encode_uint16(s) big_endian((uint16_t)(s))
#define decode_uint16(s) big_endian((uint16_t)(s))
#define encode_uint32(s) big_endian((uint32_t)(s))
#define decode_uint32(s) big_endian((uint32_t)(s))
#define encode_uint64(s) big_endian((uint64_t)(s))
#define decode_uint64(s) big_endian((uint64_t)(s))

#endif
