/*
Copyright (c) 2012-2015 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "serv.h"
#include "ssdb/ssdb.h"
#include "net/link.h"
#include "net/resp.h"
#include "util/log.h"
#include "util/bitmap.h"
#include "util/strings.h"
#include "cluster_store.h"
#include "cluster_migrate.h"
#include "range_migrate.h"
#include "ssdb/version.h"

Cluster::Cluster(SSDBServer *server){
	log_debug("Cluster init");
	this->next_id = 1;
	this->db = server->ssdb;
	this->store = new ClusterStore(server->ssdb);
}

Cluster::~Cluster(){
	delete store;
	log_debug("Cluster finalized");
}

int Cluster::init(){
	/*int ret = this->store->load_kv_node_list(&kv_node_list);
	if(ret == -1){
		log_error("load_kv_node_list failed!");
		return -1;
	}
	std::vector<Node>::iterator it;
	for(it=kv_node_list.begin(); it!=kv_node_list.end(); it++){
		const Node &node = *it;
		if(node.id >= this->next_id){
			this->next_id = node.id + 1;
		}
	}*/
	return 0;
}

int Cluster::add_kv_node(const std::string &ip, int port){
	Locking l(&mutex);
	Node node;
	node.id = next_id ++;
	node.ip = ip;
	node.port = port;

	if(store->save_kv_node(node) == -1){
		return -1;
	}

	kv_node_list.push_back(node);
	return node.id;
}

int Cluster::del_kv_node(int id){
	Locking l(&mutex);
	std::vector<Node>::iterator it;
	for(it=kv_node_list.begin(); it!=kv_node_list.end(); it++){
		const Node &node = *it;
		if(node.id == id){
			if(store->del_kv_node(id) == -1){
				return -1;
			}
			kv_node_list.erase(it);
			return 1;
		}
	}
	return 0;
}

int Cluster::set_kv_range(int id, const KeyRange &range){
	Locking l(&mutex);
	std::vector<Node>::iterator it;
	for(it=kv_node_list.begin(); it!=kv_node_list.end(); it++){
		Node &node = *it;
		if(node.id != id && node.status == Node::SERVING){
			if(node.range.overlapped(range)){
				log_error("range overlapped!");
				return -1;
			}
		}
	}

	Node *node = this->get_kv_node_ref(id);
	if(!node){
		return 0;
	}
	node->range = range;
	if(store->save_kv_node(*node) == -1){
		return -1;
	}
	return 1;
}

int Cluster::set_kv_status(int id, int status){
	Locking l(&mutex);
	Node *node = this->get_kv_node_ref(id);
	if(!node){
		return 0;
	}
	node->status = status;
	if(store->save_kv_node(*node) == -1){
		return -1;
	}
	return 1;
}

int Cluster::get_kv_node_list(std::vector<Node> *list){
	Locking l(&mutex);
	*list = kv_node_list;
	return 0;
}

Node* Cluster::get_kv_node_ref(int id){
	std::vector<Node>::iterator it;
	for(it=kv_node_list.begin(); it!=kv_node_list.end(); it++){
		Node &node = *it;
		if(node.id == id){
			return &node;
		}
	}
	return NULL;
}

int Cluster::get_kv_node(int id, Node *ret){
	Locking l(&mutex);
	Node *node = this->get_kv_node_ref(id);
	if(node){
		*ret = *node;
		return 1;
	}
	return 0;
}

int64_t Cluster::migrate_kv_data(int src_id, int dst_id, int num_keys){
	Locking l(&mutex);

	Node *src = this->get_kv_node_ref(src_id);
	if(!src){
		return -1;
	}
	Node *dst = this->get_kv_node_ref(dst_id);
	if(!dst){
		return -1;
	}

	ClusterMigrate migrate;
	int64_t size = migrate.migrate_kv_data(src, dst, num_keys);
	if(size > 0){
		if(store->save_kv_node(*src) == -1){
			log_error("after migrate_kv_data, save src failed!");
			return -1;
		}
		if(store->save_kv_node(*dst) == -1){
			log_error("after migrate_kv_data, save dst failed!");
			return -1;
		}
	}
	return size;
}

ClusterNode::ClusterNode(const std::string &ip, int port, const std::string &tag) {
	this->ip = ip;
	this->port = port;
	this->tag = tag;
	this->owner = NULL;
}

ClusterNode::~ClusterNode() {

}

ReplicationSet *ClusterNode::get_owner() const {
	return owner;
}

void ClusterNode::ref(ReplicationSet *owner) {
	this->owner = owner;
}

void ClusterNode::unref() {
	this->owner = NULL;
}

bool ClusterNode::is_master() const {
	if(owner == NULL) {
		return 0;
	}
	return owner->get_master() == this;
}

bool ClusterNode::is_stand_alone() const {
	return owner == NULL;
}

std::string ClusterNode::get_ip() const {
	return ip;
}

int ClusterNode::get_port() const {
	return port;
}

std::string ClusterNode::get_tag() const {
	return tag;
}

void ClusterNode::set_tag(const std::string &tag) {
	this->tag = tag;
}

ReplicationSet::ReplicationSet() {

}

ReplicationSet::~ReplicationSet() {

}

ClusterNode *ReplicationSet::get_slave(const std::string &ip, int port) {
	std::vector<ClusterNode*>::iterator it = slaves.begin();
	for(; it != slaves.end(); ++it) {
		if ((*it)->get_ip() == ip && (*it)->get_port() == port) {
			return (*it);
		}
	}
	return NULL;
}

ClusterNode *ReplicationSet::get_slave(int i) const {
	return i < slaves.size() ? slaves[i] : NULL;
}

ClusterNode *ReplicationSet::get_slave_by_tag(const std::string &tag) {
	std::vector<ClusterNode*>::iterator it = slaves.begin();
	for(; it != slaves.end(); ++it) {
		if((*it)->get_tag() == tag) {
			return (*it);
		}
	}
	return NULL;
}

int ReplicationSet::add_slave(ClusterNode *n) {
	if(get_slave(n->get_ip(), n->get_port()) != NULL
		|| (n->get_ip() == master->get_ip()
			&& n->get_port() == master->get_port())) {
		return -1;
	}
	n->ref(this);
	slaves.push_back(n);
	return 0;
}

int ReplicationSet::add_slave(const std::string &ip, int port, std::string &tag) {
	if(get_slave(ip, port) != NULL
		|| (ip == master->get_ip()
			&& port == master->get_port())) {
		return -1;
	}
	ClusterNode *n = new ClusterNode(ip, port, tag);
	n->ref(this);
	slaves.push_back(n);
	return 0;
}

int ReplicationSet::del_slave(const std::string &ip, int port) {
	std::vector<ClusterNode*>::iterator it = slaves.begin();
	for(; it != slaves.end(); ++it) {
		if((*it)->get_ip() == ip && (*it)->get_port() == port) {
			(*it)->unref();
			slaves.erase(it);
			return 0;
		}
	}
	return -1;
}

int ReplicationSet::del_slave(ClusterNode *n) {
	return del_slave(n->get_ip(), n->get_port());
}

int ReplicationSet::del_slave(int i) {
	if (i >= slaves.size()) {
		return -1;
	}

	int count = 0;
	std::vector<ClusterNode*>::iterator it = slaves.begin();
	for(; it != slaves.end(); ++it) {
		if (i == count++) {
			(*it)->unref();
			slaves.erase(it);
			break;
		}
	}
	return 0;
}

int ReplicationSet::slot_num() const {
	return numslots;
}

int ReplicationSet::slave_num() const {
	return slaves.size();
}

void ReplicationSet::set_slot(int s) {
	bitmapSetBit(slots, s);
}

int ReplicationSet::get_slot(int s) {
	return bitmapTestBit(slots, s);
}

void ReplicationSet::del_slot(int s) {
	bitmapClearBit(slots, s);
}

ClusterNode *ReplicationSet::get_master() const {
	return master;
}

void ReplicationSet::set_master(ClusterNode *master) {
	this->master = master;
}

#define SSDB_MIGRATING_SLOT_KEY "\xff\xff\xff\xff\xff|MIGRATING_SLOT|KV"
#define SSDB_IMPORTING_SLOT_KEY "\xff\xff\xff\xff\xff|IMPORTING_SLOT|KV"
#define SSDB_SLOTS_MAP_KEY       "\xff\xff\xff\xff\xff|SLOTS_MAP|KV"

SSDBCluster::SSDBCluster(SSDBServer *server) : server(server), migrating_slot(-1) {
	db = this->server->ssdb;
	myself = new ClusterNode(server->local_ip, server->local_port, server->local_tag);
	migrator = new RangeMigrate(server->binlog, server->ssdb, server->expiration, m_key_lock);
}

SSDBCluster::~SSDBCluster() {
	SAFE_DELETE(myself);
	SAFE_DELETE(migrator);
}

void SSDBCluster::migrate_slot(Link *link, Response *resp, int16_t slot,
		const std::string &ip, int port, int64_t timeout_ms, int64_t speed) {
	std::string start;
	start.append(SSDB_VERSION_KEY_PREFIX, sizeof(SSDB_VERSION_KEY_PREFIX));
	start.append((char*)&slot, sizeof(slot));

	std::string end;
	end.append(SSDB_VERSION_KEY_PREFIX, sizeof(SSDB_VERSION_KEY_PREFIX));
	end.append(1, '\xff');
	end.append((char*)&slot, sizeof(slot));
	migrator->migrate(link, resp, ip, port, start, end, speed, timeout_ms);
}

void SSDBCluster::import_slot(Link *link, const std::string &sync_key) {
	migrator->import(link, sync_key);
}

std::string SSDBCluster::get_migrate_result() {
	int ret = migrator->get_migrate_ret();
	return migrator->get_migrate_msg(ret);
}

int SSDBCluster::flag_migrating(int16_t slot, const std::string &to_ip, int to_port) {
	/* check if this node is the master */
	if(!myself->is_master()) {
		log_warn("I'm not the master, reject migrating");
		return -1;
	}
	/* ignore the slot not in this node */
	if(slots[slot] != myself->get_owner()) {
		log_warn("slot %d isn't my business", slot);
		return -1;
	}

	ReplicationSet *to = get_replset_by_host(to_ip, to_port);
	if(to == NULL) {
		log_warn("node %s:%d is not in the cluster", to_ip.c_str(), to_port);
		return -1;
	}
	/* check duplication */
	if(to == myself->get_owner()) {
		log_warn("migrate destination is me");
		return -1;
	}

	/* check if is in migrating */
	if(migrating_slots_to[slot] != NULL) {
		log_warn("slot %d has already been in migrating to %s:%d %s", slot,
			(to->get_master()->get_ip()).c_str(),
			to->get_master()->get_port(),
			(to->get_master()->get_tag()).c_str());
		return -1;
	}
	migrating_slots_to[slot] = to;
	log_info("flag migrating slot %d to %s:%d %s", slot,
			(to->get_master()->get_ip()).c_str(),
			to->get_master()->get_port(),
			(to->get_master()->get_tag()).c_str());
	return 0;
}

int SSDBCluster::clear_migrating(int16_t slot) {
	/* check if is in migrating */
	if(migrating_slots_to[slot] == NULL) {
		log_warn("slot %d isn't in migrating", slot);
		return -1;
	}
	/* change source and destinationg slot bitmap */
	ReplicationSet *to = migrating_slots_to[slot];
	myself->get_owner()->del_slot(slot);
	to->set_slot(slot);
	slots[slot] = to;
	migrating_slots_to[slot] = NULL;
	log_info("clear migrating slot %d to %s:%d %s", slot,
			(to->get_master()->get_ip()).c_str(),
			to->get_master()->get_port(),
			(to->get_master()->get_tag()).c_str());
	return 0;
}

int SSDBCluster::flag_importing(int16_t slot, const std::string &from_ip, int from_port) {
	/* check if this node is the master */
	if(!myself->is_master()) {
		log_warn("I'm not the master, reject importing");
		return -1;
	}
	/* ignore the slot in this node */
	if(slots[slot] == myself->get_owner()) {
		log_warn("slot %d is mine, no need to import", slot);
		return -1;
	}

	ReplicationSet *from = get_replset_by_host(from_ip, from_port);
	if(from == NULL) {
		log_warn("node %s:%d is not in the cluster", from_ip.c_str(), from_port);
		return -1;
	}
	/* check duplication */
	if(from == myself->get_owner()) {
		log_warn("import source node is me");
		return -1;
	}

	/* check if is in importing */
	if(importing_slots_from[slot] != NULL) {
		log_warn("slot %d has already been in importing from %s:%d %s", slot,
			(from->get_master()->get_ip()).c_str(),
			from->get_master()->get_port(),
			(from->get_master()->get_tag()).c_str());
		return -1;
	}
	importing_slots_from[slot] = from;
	log_info("flag migrating slot %d to %s:%d %s", slot,
			(from->get_master()->get_ip()).c_str(),
			from->get_master()->get_port(),
			(from->get_master()->get_tag()).c_str());
	return 0;
}

int SSDBCluster::clear_importing(int16_t slot) {
	/* check if is in importing */
	if(importing_slots_from[slot] == NULL) {
		log_warn("slot %d isn't in importing", slot);
		return -1;
	}
	/* change source and destinationg slot bitmap */
	ReplicationSet *from = importing_slots_from[slot];
	from->del_slot(slot);
	myself->get_owner()->set_slot(slot);
	slots[slot] = myself->get_owner();
	importing_slots_from[slot] = NULL;
	log_info("clear importing slot %d from %s:%d %s", slot,
		(from->get_master()->get_ip()).c_str(),
		from->get_master()->get_port(),
		(from->get_master()->get_tag()).c_str());
	return 0;
}

int SSDBCluster::flag_normal(int16_t slot){
	if(slot_importing(slot) == 1) {
		return clear_importing(slot);
	}
	if(slot_migrating(slot) == 1) {
		return clear_migrating(slot);
	}
	return 0;
}

int SSDBCluster::slot_importing(int16_t slot) const {
	return importing_slots_from[slot] == NULL ? 0 : 1;
}

int SSDBCluster::slot_migrating(int16_t slot) const {
	return migrating_slots_to[slot] == NULL ? 0 : 1;
}

int SSDBCluster::integrity_check() const {
	for(int i = 0; i < CLUSTER_SLOTS; ++i) {
		if(slots[i] == NULL) {
			log_warn("cluster integrity check failed, slot %d has no owner", i);
			return -1;
		}
		if(slots[i]->get_master() != NULL) {
			log_warn("cluster integrity check failed, owner of slot %d has no master", i);
			return -1;
		}
	}
	return 0;
}

int SSDBCluster::init() {
	/* load migrating_slot */
	std::string ms;
	std::string key = SSDB_MIGRATING_SLOT_KEY;
	int16_t reserve_slot = -1;
	key.append((char*)&reserve_slot, sizeof(reserve_slot));
	int ret = db->raw_get(key, &ms);
	if(ret == -1) {
		return -1;
	}
	if(ret == 0) {
		migrating_slot = -1;
	} else {
		migrating_slot = str_to_int64(ms);
	}

	/* load importing_slots */
	key = SSDB_IMPORTING_SLOT_KEY;
	key.append((char*)&reserve_slot, sizeof(reserve_slot));
	std::string is;
	ret = db->raw_get(key, &is);
	if(ret == -1) {
		return -1;
	}
	if(is.size() == sizeof(importing_slots)) {
		memcpy(importing_slots, is.c_str(), is.size());
	}

	/* load slots map */
	if(init_slot() == -1) {
		return -1;
	}
	return 0;
}

ClusterNode *SSDBCluster::me() const {
	return myself;
}

/*KeyLock &SSDBCluster::get_key_lock() {
	return key_lock;
}*/

KeyLock &SSDBCluster::get_key_lock(const std::string& key) {
	return m_key_lock.get_key_lock(key);
}

/*RWLock &SSDBCluster::get_state_lock() {
	return state_lock;
}*/

RWLock &SSDBCluster::get_state_lock(int16_t slot) {
	return m_state_lock.get_rw_lock(slot);
}

int SSDBCluster::set_slot_migrating(int16_t slot) {
	std::string key = SSDB_MIGRATING_SLOT_KEY;
	int16_t reserve_slot = -1;
	key.append((char*)&reserve_slot, sizeof(reserve_slot));
	int ret = db->raw_set(key, str(slot));
	if(ret == -1) {
		return -1;
	}
	migrating_slot = slot;
	return 0;
}

int SSDBCluster::get_slot_migrating(int16_t *slot) {
	*slot = migrating_slot;
	return 0;
}

int SSDBCluster::unset_slot_migrating() {
	std::string key = SSDB_MIGRATING_SLOT_KEY;
	int16_t reserve_slot = -1;
	key.append((char*)&reserve_slot, sizeof(reserve_slot));
	int ret = db->raw_set(key, str(int16_t(-1)));
	if(ret == -1) {
		return -1;
	}
	int retval = migrating_slot >= 0 ? 1 : 0;
	migrating_slot = -1;
	return retval;
}

int SSDBCluster::set_slot_importing(int16_t slot) {
	std::string im;
	std::string key = SSDB_IMPORTING_SLOT_KEY;
	int16_t reserve_slot = -1;
	key.append((char*)&reserve_slot, sizeof(reserve_slot));
	bitmapSetBit(importing_slots, slot);
	int ret = db->raw_set(key, std::string((const char*)importing_slots, sizeof(importing_slots)));
	if(ret == -1) {
		return -1;
	}
	return 0;
}

int SSDBCluster::test_slot_importing(int16_t slot, int *flag) {
	*flag = bitmapTestBit(importing_slots, slot);
	return 0;
}

int SSDBCluster::unset_slot_importing(int16_t slot) {
	std::string im;
	std::string key = SSDB_IMPORTING_SLOT_KEY;
	int16_t reserve_slot = -1;
	key.append((char*)&reserve_slot, sizeof(reserve_slot));
	int retval = bitmapTestBit(importing_slots, slot);
	bitmapClearBit(importing_slots, slot);
	int ret = db->raw_set(key, std::string((const char*)importing_slots, sizeof(importing_slots)));
	if(ret == -1) {
		return -1;
	}
	return retval;
}

int SSDBCluster::init_slot() {
	std::string val;
	std::string key = SSDB_SLOTS_MAP_KEY;
	int16_t reserve_slot = -1;
	key.append((char*)&reserve_slot, sizeof(reserve_slot));
	int ret = server->ssdb->raw_get(key, &val);
	if(ret == -1) {
		return -1;
	}
	if(val.size() == sizeof(slots_map)) {
		memcpy(slots_map, val.c_str(), val.size());
	}
	return 0;
}

int SSDBCluster::reset_slot() {
	memset(slots_map, 0, sizeof(slots_map));
	std::string key = SSDB_SLOTS_MAP_KEY;
	int16_t reserve_slot = -1;
	key.append((char*)&reserve_slot, sizeof(reserve_slot));
	int ret = server->ssdb->raw_del(key);
	if (ret == -1) {
		return -1;
	}
	return 0;
}

std::vector<int> SSDBCluster::slot_range() {
	std::vector<int> range;
	int end = -1;
	for(int i = 0; i < CLUSTER_SLOTS; ++i) {
		int bit = bitmapTestBit(slots_map, i);
		log_debug("slot %d:%d", i, bit);
		if(bit) {
			if(end == -1) {
				range.push_back(i);
			}
			end = i;
		} else {
			if(end != -1) {
				range.push_back(end);
				end = -1;
			}
		}
	}
	if(end != -1) {
		range.push_back(end);
	}
	return range;
}

int SSDBCluster::set_slot(int16_t slot) {
	std::string key = SSDB_SLOTS_MAP_KEY;
	int16_t reserve_slot = -1;
	key.append((char*)&reserve_slot, sizeof(reserve_slot));
	bitmapSetBit(slots_map, slot);
	std::string val((const char*)slots_map, sizeof(slots_map));
	int ret = server->ssdb->raw_set(key, val);
	if(ret == -1) {
		return -1;
	}
	server->binlog->write(BinlogType::SYNC, BinlogCommand::RAW, key, val);
	return 0;
}

int SSDBCluster::unset_slot(int16_t slot) {
	std::string key = SSDB_SLOTS_MAP_KEY;
	int16_t reserve_slot = -1;
	key.append((char*)&reserve_slot, sizeof(reserve_slot));
	bitmapClearBit(slots_map, slot);
	std::string val((const char*)slots_map, sizeof(slots_map));
	int ret = server->ssdb->raw_set(key, val);
	if(ret == -1) {
		return -1;
	}
	server->binlog->write(BinlogType::SYNC, BinlogCommand::RAW, key, val);
	return 0;
}

int SSDBCluster::test_slot(int16_t slot, int *flag) {
	*flag = bitmapTestBit(slots_map, slot);
	return 0;
}

int SSDBCluster::build_cluster(const std::string &config) {
	// TODO
	return 0;
}

ReplicationSet *SSDBCluster::get_replset_by_slot(int16_t slot) {
	std::vector<ReplicationSet*>::iterator it = replications.begin();
	for(; it != replications.end(); ++it) {
		if((*it)->get_slot(slot)) {
			return *it;
		}
	}
	/* incompleted cluster */
	return NULL;
}

ReplicationSet *SSDBCluster::get_replset_by_host(const std::string &ip, int port) {
	std::vector<ReplicationSet*>::iterator it = replications.begin();
	for(; it != replications.end(); ++it) {
		ClusterNode *master = (*it)->get_master();
		if(master == NULL) {
			return NULL;
		}
		if(master->get_ip() == ip && master->get_port() == port) {
			return *it;
		}
		if((*it)->get_slave(ip, port) != NULL) {
			return *it;
		}
	}
	/* node is not in cluster */
	return NULL;
}
