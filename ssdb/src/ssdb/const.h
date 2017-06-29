/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_CONST_H_
#define SSDB_CONST_H_

static const int SSDB_SCORE_WIDTH		= 9;
static const int SSDB_KEY_LEN_MAX		= 255;

class DataType{
public:
	static const char SYNCLOG	= 1;
	static const char KV		= 'k';
	static const char HASH		= 'h'; // hashmap(sorted by key)
	static const char HSIZE		= 'H';
	static const char ZSET		= 's'; // key => score
	static const char ZSCORE	= 'z'; // key|score => ""
	static const char ZSIZE		= 'Z';
	static const char SET		= 'm';
	static const char SSIZE		= 'M';
	static const char QUEUE		= 'q';
	static const char QSIZE		= 'Q';
	static const char MIN_PREFIX = HASH;
	static const char MAX_PREFIX = ZSET;
};

class BinlogType{
public:
	static const char NOOP		= 0;
	static const char SYNC		= 1;
	static const char MIRROR	= 2;
	static const char COPY		= 3;
};

#define SSDB_SYNC_CMD_MIN (unsigned char)BinlogCommand::RAW
#define SSDB_SYNC_CMD_MAX (unsigned char)BinlogCommand::S_DEL

class BinlogCommand{
public:
	static const char NONE  = 0;
	static const char KSET  = 1;
	static const char KDEL  = 2;
	static const char HSET  = 3;
	static const char HDEL  = 4;
	static const char ZSET  = 5;
	static const char ZDEL  = 6;

	static const char QPUSH_BACK	= 10;
	static const char QPUSH_FRONT	= 11;
	static const char QPOP_BACK		= 12;
	static const char QPOP_FRONT	= 13;
	static const char QSET			= 14;

	static const char SSET = 15;
	static const char SDEL = 16;

	static const char BEGIN	= 7;
	static const char END	= 8;
	static const char ACK	= 9;

	static const char STOP		= 125;
	static const char DESC		= 126;
	static const char ROTATE	= 127;

	/* 4 new version binlog && replicate */
	// RAW
	static const char RAW   = 140;

	// KV
	static const char K_SET			= 150;
	static const char K_DEL			= 151;
	static const char K_INCR		= 152;
	static const char K_DECR		= 153;
	static const char K_EXPIRE		= 154;
	static const char K_EXPIRE_AT	= 155;
	static const char K_SETBIT		= 156;

	// HASH
	static const char H_SET		= 170;
	static const char H_DEL		= 171;
	static const char H_CLEAR	= 172;
	static const char H_INCR	= 173;
	static const char H_DECR	= 174;

	// ZSET
	static const char Z_SET			= 190;
	static const char Z_DEL			= 191;
	static const char Z_CLEAR		= 192;
	static const char Z_INCR		= 193;
	static const char Z_DECR		= 194;
	static const char Z_POP_FRONT	= 195;
	static const char Z_POP_BACK	= 196;

	// QUEUE
	static const char Q_PUSH_FRONT	= 210;
	static const char Q_PUSH_BACK	= 211;
	static const char Q_POP_FRONT	= 212;
	static const char Q_POP_BACK	= 213;
	static const char Q_FIX			= 214;
	static const char Q_CLEAR		= 215;
	static const char Q_SET			= 216;

	// SET
	static const char S_SET		= 230;
	static const char S_DEL		= 231;
	static const char S_CLEAR   = 232;
};

#endif
