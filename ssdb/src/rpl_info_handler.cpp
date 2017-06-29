/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "net/fde.h"
#include "util/log.h"
#include "slave.h"
#include "include.h"
#include "rpl_info_handler.h"

static const std::string RPL_INFO_MASTER_IP = "\xff\xff\xff\xff\xff|MASTER_IP|KV";
static const std::string RPL_INFO_MASTER_PORT = "\xff\xff\xff\xff\xff|MASTER_PORT|KV";
static const std::string RPL_INFO_LAST_SEQ = "\xff\xff\xff\xff\xff|MASTER_LAST_SEQ|KV";
static const std::string RPL_INFO_LAST_KEY = "\xff\xff\xff\xff\xff|MASTER_LAST_KEY|KV";
static const std::string RPL_INFO_BINLOG_SEQ = "\xff\xff\xff\xff\xff|BINLOG_SEQ|KV";
static const std::string RPL_INFO_GROUP_ID = "\xff\xff\xff\xff\xff|GROUP_ID|KV";
static const std::string RPL_INFO_SERVER_ID = "\xff\xff\xff\xff\xff|SERVER_ID|KV";
static const std::string RPL_INFO_AUTH = "\xff\xff\xff\xff\xff|AUTH|KV";

#define VERIFY_META { assert (this->m_meta); }

int RplInfoHandler::get_master_ip(std::string &ip) {
	VERIFY_META;
	return this->m_meta->get(RPL_INFO_MASTER_IP, &ip, 0);
}

int RplInfoHandler::save_master_ip(const std::string &ip) {
	VERIFY_META;
	Transaction trans(this->m_meta, RPL_INFO_MASTER_IP);
	return this->m_meta->set(RPL_INFO_MASTER_IP, ip, trans, 0);
}

int RplInfoHandler::get_master_port(int32_t &port) {
	VERIFY_META;

	std::string val;
	int ret = this->m_meta->get(RPL_INFO_MASTER_PORT, &val, 0);
	if (ret == -1) { /* get error */
		return -1;
	}
	if (ret == 1) { /* get data */
		port = str_to_int(val);
	}
	return ret;
}

int RplInfoHandler::save_master_port(int32_t port) {
	VERIFY_META;
	Transaction trans(this->m_meta, RPL_INFO_MASTER_PORT);
	return this->m_meta->set(RPL_INFO_MASTER_PORT, str(port), trans, 0);
}

int RplInfoHandler::get_master_last_seq(uint64_t &seq) {
	VERIFY_META;

	std::string val;
	int ret = this->m_meta->get(RPL_INFO_LAST_SEQ, &val, 0);
	if (ret == -1) {
		return -1;
	}
	if (ret == 1) {
		seq = str_to_uint64(val);
	}
	return ret;
}

int RplInfoHandler::save_master_last_seq(uint64_t seq) {
	VERIFY_META;
	Transaction trans(this->m_meta, RPL_INFO_LAST_SEQ);
	return this->m_meta->set(RPL_INFO_LAST_SEQ, str(seq), trans, 0);
}

int RplInfoHandler::get_master_last_key(std::string &key) {
	VERIFY_META;
	return this->m_meta->get(RPL_INFO_LAST_KEY, &key, 0);
}

int RplInfoHandler::save_master_last_key(const std::string &key) {
	VERIFY_META;
	Transaction trans(this->m_meta, RPL_INFO_LAST_KEY);
	return this->m_meta->set(RPL_INFO_LAST_KEY, key, trans, 0);
}

int RplInfoHandler::get_binlog_seq(uint64_t &seq) {
	VERIFY_META;

	std::string val;
	int ret = this->m_meta->get(RPL_INFO_BINLOG_SEQ, &val, 0);
	if (ret == -1) {
		return -1;
	}
	if (ret == 1) {
		seq = str_to_uint64(val);
	}
	return ret;
}

int RplInfoHandler::save_binlog_seq(uint64_t seq) {
	VERIFY_META;
	Transaction trans(this->m_meta, RPL_INFO_BINLOG_SEQ);
	return this->m_meta->set(RPL_INFO_BINLOG_SEQ, str(seq), trans, 0);
}

int RplInfoHandler::get_group_id(int32_t &group_id) {
	VERIFY_META;

	std::string val;
	int ret = this->m_meta->get(RPL_INFO_GROUP_ID, &val, 0);
	if (ret == -1) {
		return -1;
	}
	if (ret == 1) {
		group_id = str_to_int(val);
	}
	return ret;
}

int RplInfoHandler::save_group_id(int32_t group_id) {
	VERIFY_META;
	Transaction trans(this->m_meta, RPL_INFO_GROUP_ID);
	return this->m_meta->set(RPL_INFO_GROUP_ID, str(group_id), trans, 0);
}

int RplInfoHandler::get_server_id(int32_t &server_id) {
	VERIFY_META;

	std::string val;
	int ret = this->m_meta->get(RPL_INFO_SERVER_ID, &val, 0);
	if (ret == -1) {
		return -1;
	}
	if (ret == 1) {
		server_id = str_to_int(val);
	}
	return ret;
}

int RplInfoHandler::save_server_id(int32_t server_id) {
	VERIFY_META;
	Transaction trans(this->m_meta, RPL_INFO_SERVER_ID);
	return this->m_meta->set(RPL_INFO_SERVER_ID, str(server_id), trans, 0);
}

int RplInfoHandler::get_auth(std::string &auth) {
	VERIFY_META;
	return this->m_meta->get(RPL_INFO_AUTH, &auth, 0);
}

int RplInfoHandler::save_auth(const std::string &auth) {
	VERIFY_META;
	Transaction trans(this->m_meta, RPL_INFO_AUTH);
	return this->m_meta->set(RPL_INFO_AUTH, auth, trans, 0);
}
