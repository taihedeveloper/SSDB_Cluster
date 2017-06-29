/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "net/fde.h"
#include "util/log.h"
#include "slave.h"
#include "include.h"
#include "rpl_mi.h"

MasterInfo::MasterInfo(RplInfoHandler *handler_)
	: handler(handler_), port(-1), group_id(-1)
	, server_id(-1), last_seq(0) { }

MasterInfo::~MasterInfo() {
	if (handler) { delete handler; }
}

#define RETURN_ON_ERROR { if (ret == -1) return ret; }

int MasterInfo::load() {
	assert (handler);

	int ret = this->handler->get_master_ip(this->ip);
	RETURN_ON_ERROR;

	ret = this->handler->get_master_port(this->port);
	RETURN_ON_ERROR;

	ret = this->handler->get_master_last_seq(this->last_seq);
	RETURN_ON_ERROR;

	ret = this->handler->get_master_last_key(this->last_key);
	RETURN_ON_ERROR;

	ret = this->handler->get_auth(this->auth);
	RETURN_ON_ERROR;

	ret = this->handler->get_group_id(this->group_id);
	RETURN_ON_ERROR;

	ret = this->handler->get_server_id(this->server_id);
	RETURN_ON_ERROR;

	return ret;
}

int MasterInfo::save() {
	assert (handler);

	int ret = this->handler->save_master_ip(this->ip);
	RETURN_ON_ERROR;

	ret = this->handler->save_master_port(this->port);
	RETURN_ON_ERROR;

	ret = this->handler->save_master_last_seq(this->last_seq);
	RETURN_ON_ERROR;

	ret = this->handler->save_master_last_key(this->last_key);
	RETURN_ON_ERROR;

	ret = this->handler->save_auth(this->auth);
	RETURN_ON_ERROR;

	ret = this->handler->save_group_id(this->group_id);
	RETURN_ON_ERROR;

	ret = this->handler->save_server_id(this->server_id);
	RETURN_ON_ERROR;

	return ret;
}

