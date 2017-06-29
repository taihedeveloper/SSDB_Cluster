#ifndef RPL_INFO_HANDLER_H_
#define RPL_INFO_HANDLER_H_

#include <stdint.h>
#include <string>
#include <pthread.h>
#include <vector>
#include "ssdb/ssdb_impl.h"
#include "net/link.h"

class RplInfoHandler {
private:
	SSDB *m_meta;

public:
	RplInfoHandler(SSDB *meta) : m_meta(meta) { }
	~RplInfoHandler() { }

public:
	/* master ip port */
	int get_master_ip(std::string &ip);
	int get_master_port(int32_t &ip);
	int save_master_ip(const std::string &ip);
	int save_master_port(int32_t port);

	/* auth */
	int get_auth(std::string &auth);
	int save_auth(const std::string &auth);

	/* last master seq/key we get */
	int get_master_last_seq(uint64_t &seq);
	int get_master_last_key(std::string &key);
	int save_master_last_seq(uint64_t seq);
	int save_master_last_key(const std::string &key);

	/* binlog info */
	int get_binlog_seq(uint64_t &seq);
	int save_binlog_seq(uint64_t seq);

	/* group-id server-id */
	int get_group_id(int32_t &group_id);
	int save_group_id(int32_t group_id);
	int get_server_id(int32_t &server_id);
	int save_server_id(int32_t server_id);
};

#endif
