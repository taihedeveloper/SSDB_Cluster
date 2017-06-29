#ifndef RPL_MI_H_
#define RPL_MI_H_

#include <stdint.h>
#include <string>
#include <pthread.h>
#include <vector>
#include "ssdb/ssdb_impl.h"
#include "net/link.h"
#include "rpl_info_handler.h"

class MasterInfo {
public:
	RplInfoHandler *handler;
	std::string ip;
	int32_t port;
	int32_t group_id;
	int32_t server_id;
	std::string auth;

	uint64_t last_seq;
	std::string last_key;

	bool inited;

	MasterInfo(RplInfoHandler *handler_);
	~MasterInfo();

	int load();
	int save();

	int load_last_seq() { return handler->get_master_last_seq(last_seq); }
	int save_last_seq() { return handler->save_master_last_seq(last_seq); }
	int load_last_key() { return handler->get_master_last_key(last_key); }
	int save_last_key() { return handler->save_master_last_key(last_key); }


};

#endif
