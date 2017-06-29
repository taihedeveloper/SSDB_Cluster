/*
Copyright (c) 2012-2015 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_CLUSTER_H_
#define SSDB_CLUSTER_H_

#include "include.h"
#include <string>
#include <vector>
#include <set>
#include "util/strings.h"
#include "util/thread.h"
#include "util/spin_lock.h"

class KeyRange{
public:
	std::string begin;
	std::string end;
	KeyRange(){
	}
	KeyRange(const std::string &begin, const std::string &end){
		this->begin = begin;
		this->end = end;
	}
	std::string str() const{
		return "(\"" + str_escape(begin) + "\" - \"" + str_escape(end) + "\"]";
	}
	bool overlapped(const KeyRange &range) const{
		if(!this->begin.empty() && !range.end.empty() && this->begin >= range.end){
			return false;
		}
		if(!this->end.empty() && !range.begin.empty() && this->end <= range.begin){
			return false;
		}
		return true;
	}
	bool empty() const{
		return begin == "" && end == "";
	}
};

class Node{
public:
	const static int INIT    = 0;
	const static int SERVING = 1;

	int id;
	int status;
	KeyRange range;
	std::string ip;
	int port;

	Node(){
		this->id = 0;
		this->status = INIT;
	}
	std::string str() const{
		char buf[512];
		snprintf(buf, sizeof(buf), "%4d: %s", id, str_escape(range.str()).c_str());
		return std::string(buf);
	}
};

class SSDB;
class SSDBServer;
class ClusterStore;
class Cluster {
public:
	Cluster(SSDBServer *server);
	~Cluster();

	int init();

	int get_kv_node(int id, Node *ret);
	// 返回节点的 id
	int add_kv_node(const std::string &ip, int port);
	int del_kv_node(int id);
	int set_kv_range(int id, const KeyRange &range);
	int set_kv_status(int id, int status);
	int get_kv_node_list(std::vector<Node> *list);
	// 返回迁移的字节数
	int64_t migrate_kv_data(int src_id, int dst_id, int num_keys);


private:
	SSDB *db;
    SSDBServer *server;
	ClusterStore *store;
	int next_id;
	std::vector<Node> kv_node_list;
	Mutex mutex;

	Node* get_kv_node_ref(int id);
};

class ReplicationSet;
class ClusterNode {
public:
	ClusterNode(const std::string &ip, int port, const std::string &tag);
	~ClusterNode();

	ReplicationSet *get_owner() const;            /* get the replication set this node belong to */
	void ref(ReplicationSet *owner);              /* set the owner-ship */
	void unref();                                 /* clear the owner-ship */
	bool is_master() const;                       /* check if is a master */
	bool is_stand_alone() const;                  /* check if is a stand alone */
	std::string get_ip() const;                   /* get the node's ip */
	int get_port() const;                         /* get the node's port */
	std::string get_tag() const;                  /* get the node's tag */
	void set_tag(const std::string &tag);         /* set the node's tag */
	std::string str() const;                      /* serialization */

private:
	ReplicationSet *owner;                        /* this replication set owner this node */
	std::string ip;                               /* node ip */
	int port;                                     /* node port */
	std::string tag;                              /* node tag */
};

class ReplicationSet {
public:
	ReplicationSet();
	~ReplicationSet();

	ClusterNode* get_slave(int i) const;                             /* get the slave by id */
	ClusterNode* get_slave(const std::string &ip, int port);         /* get the slave by ip and port */
	ClusterNode* get_slave_by_tag(const std::string &tag);           /* get a slave by tag */
	int add_slave(const std::string &ip, int port, std::string &tag);/* create and add a slave */
	int add_slave(ClusterNode *n);                                   /* check exists and add slave */
	int del_slave(const std::string &ip, int port);                  /* del the slave by ip and port */
	int del_slave(ClusterNode *n);                                   /* del the slave */
	int del_slave(int i);                                            /* del slave by id */
	int slave_num() const;                                           /* get slave number */
	int slot_num() const;                                            /* get slot number of this node */
	void set_slot(int s);                                            /* flag owner-ship of a slot */
	int get_slot(int s);                                             /* test owner-ship of a slot */
	void del_slot(int s);                                            /* clean owner-ship of a slot */
	ClusterNode *get_master() const;                                 /* get the master of this node */
	void set_master(ClusterNode *master);                            /* set the master of this node */
	std::string bitmap() const;                                      /* slot bitmap */

private:
	int numslots;
	unsigned char slots[CLUSTER_SLOTS/8]; /* slot bit map */
	std::vector<ClusterNode*> slaves;     /* slave list */
	ClusterNode *master;                  /* master node */
};

class Link;
class Response;
class RangeMigrate;
class SSDBCluster {
public:
	explicit SSDBCluster(SSDBServer *server);
	~SSDBCluster();

	int flag_migrating(int16_t slot,
		const std::string &to_ip, int to_port);         /* set migrating flag */
	int clear_migrating(int16_t slot);                  /* clean migrating flag */
	int flag_importing(int16_t slot,
		const std::string &from_ip, int from_port);     /* set importing flag */
	int clear_importing(int16_t slot);                  /* clean importing flag */
	int flag_normal(int16_t slot);                      /* clean importing or migrating flag */
	int slot_importing(int16_t slot) const;             /* slot in importing ? */
	int slot_migrating(int16_t slot) const;             /* slot in migrating ? */
	int integrity_check() const;                        /* check if slots full covered and all slots own a master */
	int init();                                         /* talk with meta, get config and init */
	ClusterNode* me() const;                            /* get this node */

	ReplicationSet *get_replset_by_slot(int16_t slot);                    /* get replication set by slot */
	ReplicationSet *get_replset_by_host(const std::string &ip, int port); /* get replication set by one node */

	/* migrate */
	void migrate_slot(Link *link, Response *resp, int16_t slot,
			const std::string &ip, int port, int64_t timeout_ms, int64_t speed); /* migrate slot to ip:port at backend */
	void import_slot(Link *link, const std::string &prefix);                     /* import slot from the link an backend */
	std::string get_migrate_result();                                            /* get the result of last migration */

	/* consistency */
//	KeyLock &get_key_lock();
	KeyLock &get_key_lock(const std::string& key); /* seq */
//	RWLock &get_state_lock();
	RWLock &get_state_lock(int16_t slot);
	int set_slot_migrating(int16_t slot);
	int get_slot_migrating(int16_t *slot);
	int unset_slot_migrating();
	int test_slot_importing(int16_t slot, int *flag);
	int set_slot_importing(int16_t slot);
	int unset_slot_importing(int16_t slot);
	int set_slot(int16_t slot);
	int unset_slot(int16_t slot);
	int test_slot(int16_t slot, int *flag);
	std::vector<int> slot_range();
	int init_slot();
	int reset_slot();

private:
	/* state */
	int build_cluster(const std::string &config); /* build cluster by config */

	ReplicationSet *migrating_slots_to[CLUSTER_SLOTS];
	ReplicationSet *importing_slots_from[CLUSTER_SLOTS];
	ReplicationSet *slots[CLUSTER_SLOTS];
	ClusterNode *myself;                                 /* this node */
	std::vector<ReplicationSet*> replications;           /* replication set list */

	/* store */
	SSDB *db;
	SSDBServer *server;
	RangeMigrate *migrator;

	/* consistency */
	//KeyLock key_lock;
	//RWLock state_lock;
	SegKeyLock m_key_lock;
	SegRWLock m_state_lock;
	int16_t migrating_slot;
	unsigned char importing_slots[CLUSTER_SLOTS/8];
	unsigned char slots_map[CLUSTER_SLOTS/8];

	/* flag */
	const std::string slot_migrating_key;
	const std::string slot_importing_key;
};

#endif
