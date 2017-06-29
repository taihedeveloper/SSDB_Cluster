/*
	eransaction(backend->ssdb->binlogs);
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <pthread.h>
#include <assert.h>
#include <errno.h>
#include <string>
#include "backend_sync.h"
#include "util/log.h"
#include "util/strings.h"

BackendSync::BackendSync(SSDBImpl *ssdb, int sync_speed, uint32_t snapshot_timeout){
	thread_quit = false;
	this->ssdb = ssdb;
	this->sync_speed = sync_speed;

	this->snapshot_timeout = snapshot_timeout;
	pthread_t timer;
	if (pthread_create(&timer, NULL, &BackendSync::_timer_thread, this) != 0) {
		log_error("BackendSync start timer thread failed");
		assert (0);
	} else {
		log_info("start backendsync2 timer thread(%d)", timer);
	}
}

BackendSync::~BackendSync(){
	thread_quit = true;
	int retry = 0;
	int MAX_RETRY = 100;
	while(retry++ < MAX_RETRY){
		// there is something wrong that sleep makes other threads
		// unable to acquire the mutex
		{
			Locking l(&mutex);
			if(workers.empty()){
				break;
			}
		}
		usleep(50 * 1000);
	}
	if(retry >= MAX_RETRY){
		log_info("Backend worker not exit expectedly");
	}
	log_debug("BackendSync finalized");
}

std::vector<std::string> BackendSync::stats(){
	std::vector<std::string> ret;
	std::map<pthread_t, Client *>::iterator it;

	Locking l(&mutex);
	for(it = workers.begin(); it != workers.end(); it++){
		Client *client = it->second;
		ret.push_back(client->stats());
	}
	return ret;
}

void BackendSync::proc(const Link *link){
	log_info("fd: %d, accept sync client", link->fd());
	struct run_arg *arg = new run_arg();
	arg->link = link;
	arg->backend = this;

	pthread_t tid;
	int err = pthread_create(&tid, NULL, &BackendSync::_run_thread, arg);
	if(err != 0){
		log_error("can't create thread: %s", strerror(err));
		delete link;
		delete arg;
	}
}

/* timer snapshot */

void* BackendSync::_timer_thread(void *arg) {
	pthread_detach(pthread_self());
	BackendSync *sync = (BackendSync *)arg;

	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_lock(&mutex);
	while (!sync->thread_quit) {
		struct timespec timeout;
		timeout.tv_sec = time(NULL) + 5;
		timeout.tv_nsec = 0;

		int err = pthread_cond_timedwait(&cond, &mutex, &timeout);
		if (err == ETIMEDOUT) {

			// do period work
			sync->clear_timeout_snapshot();
		}
	}
	pthread_mutex_unlock(&mutex);
	pthread_cond_destroy(&cond);
	pthread_mutex_destroy(&mutex);
}

void BackendSync::clear_timeout_snapshot() {
	Locking l(&mutex);

	if (snapshots.empty()) return;

	log_debug("in clear_timeout_snapshot");

	std::vector<std::string> purges;
	time_t now = time(NULL);
	CopySnapshot *csnapshot;
	std::map<std::string, CopySnapshot *>::iterator it;
	for (it = snapshots.begin(); it != snapshots.end(); it++) {
		csnapshot = it->second;
		if (csnapshot && csnapshot->status != CopySnapshot::ACTIVE
				&& csnapshot->last_active + snapshot_timeout < now) {
			purges.push_back(it->first);
		}
	}

	log_debug("to be purges count(%u)", purges.size());

	for (size_t i = 0; i < purges.size(); i++) {
		log_info("clean snapshot for host(%s)", purges[i].c_str());
		it = snapshots.find(purges[i]);
		assert (it != snapshots.end());

		csnapshot = it->second;
		if (csnapshot && csnapshot->snapshot) {
			log_debug("release snapshot");
			ssdb->release_snapshot(csnapshot->snapshot);
		}
		if (csnapshot) {
			delete csnapshot;
		}
		snapshots.erase(it);
	}
}

BackendSync::CopySnapshot *BackendSync::last_snapshot(const std::string &host) {
	Locking l(&mutex);

	std::map<std::string, CopySnapshot *>::iterator it;
	it = snapshots.find(host);
	if (it != snapshots.end()) {
		CopySnapshot *csnapshot = it->second;

		if (csnapshot && csnapshot->snapshot) {
			csnapshot->status = CopySnapshot::ACTIVE;
			return csnapshot;
		}

		if (csnapshot) delete csnapshot;
		snapshots.erase(it);
	}

	return NULL;
}

BackendSync::CopySnapshot *BackendSync::create_snapshot(const std::string &host) {
	Locking l(&mutex);

	// if exists delete
	std::map<std::string, CopySnapshot *>::iterator it = snapshots.find(host);
	if (it != snapshots.end()) {
		log_warn("snapshot for host(%s) already exists, release first.", host.c_str());
		CopySnapshot *csnapshot = it->second;
		if (csnapshot && csnapshot->snapshot) {
			this->ssdb->release_snapshot(csnapshot->snapshot);
		}
		if (csnapshot) {
			delete csnapshot;
		}
		snapshots.erase(it);
	}

	//SSDBServer *server = this->owner;

	// lockdb gurantee the atomicly of create leveldb snapshot and get binlog seq.
	//server->ssdb->lock_db();

	const leveldb::Snapshot *snapshot = this->ssdb->get_snapshot();
	//uint64_t binlog_last_seq = server->binlog->get_last_seq();
	time_t now = time(NULL);

	CopySnapshot *csnapshot = new CopySnapshot;
	csnapshot->snapshot = snapshot;
	csnapshot->status = CopySnapshot::ACTIVE;
	csnapshot->last_active = now;
	//csnapshot->binlog_seq = binlog_last_seq;
	snapshots.insert(std::make_pair<std::string, CopySnapshot *>(host, csnapshot));

	log_info("create snapshot success.");

	//server->ssdb->unlock_db();

	return csnapshot;
}

void BackendSync::release_last_snapshot(const std::string &host) {
	Locking l(&mutex);

	std::map<std::string, CopySnapshot *>::iterator it;
	it = snapshots.find(host);
	if (it == snapshots.end())
		return;

	CopySnapshot *csnapshot = it->second;
	if (csnapshot && csnapshot->snapshot) {
		this->ssdb->release_snapshot(csnapshot->snapshot);
	}
	if (csnapshot) delete csnapshot;
	snapshots.erase(it);
}

void BackendSync::mark_snapshot(const std::string &host, int status) {
	Locking l(&mutex);

	std::map<std::string, CopySnapshot *>::iterator it;
	it = snapshots.find(host);
	if (it == snapshots.end())
		return;

	CopySnapshot *snapshot = it->second;
	snapshot->status = status;
	snapshot->last_active = time(NULL);
}

void* BackendSync::_run_thread(void *arg){
	pthread_detach(pthread_self());
	SET_PROC_NAME("backend_sync");
	struct run_arg *p = (struct run_arg*)arg;
	BackendSync *backend = (BackendSync *)p->backend;
	Link *link = (Link *)p->link;
	delete p;

	// set Link non block
	link->noblock(false);

	SSDBImpl *ssdb = (SSDBImpl *)backend->ssdb;
	BinlogQueue *logs = ssdb->binlogs;

	Client client(backend);
	client.link = link;
	client.init();

	{
		pthread_t tid = pthread_self();
		Locking l(&backend->mutex);
		backend->workers[tid] = &client;
	}

// sleep longer to reduce logs.find
#define TICK_INTERVAL_MS	300
#define NOOP_IDLES			(3000/TICK_INTERVAL_MS)

	int idle = 0;
	while(!backend->thread_quit){
		// TODO: test
		//usleep(2000 * 1000);

		if(client.status == Client::OUT_OF_SYNC){
			client.reset();
			client.prepare();
			continue;
		}

		if(client.status == Client::SYNC) {
			if(!client.sync(logs)) {
				if (idle >= NOOP_IDLES) {
					idle = 0;
					client.noop();
				} else {
					idle ++;
				}
				usleep(TICK_INTERVAL_MS * 1000);
			} else {
				idle = 0;
			}
		}
		if(client.status == Client::COPY){
			client.copy();
		}

		float data_size_mb = link->output->size() / 1024.0 / 1024.0;
		if(link->flush() == -1){
			log_info("%s:%d fd: %d, send error: %s", link->remote_ip, link->remote_port, link->fd(), strerror(errno));
			break;
		}
		if(backend->sync_speed > 0){
			usleep((data_size_mb / backend->sync_speed) * 1000 * 1000);
		}
	}

	// mark for breakpoint
	if (client.status == Client::COPY) {
		backend->mark_snapshot(client.host, BackendSync::CopySnapshot::ABORT);
	}

	log_info("Sync Client quit, %s:%d fd: %d, delete link", link->remote_ip, link->remote_port, link->fd());
	delete link;

	Locking l(&backend->mutex);
	backend->workers.erase(pthread_self());
	return (void *)NULL;
}

/* Client */

BackendSync::Client::Client(BackendSync *backend){
	status = Client::INIT;
	this->backend = backend;
	link = NULL;
	last_seq = 0;
	last_noop_seq = 0;
	last_key = "";
	is_mirror = false;
	iter = NULL;
}

BackendSync::Client::~Client(){
	if(iter){
		delete iter;
		iter = NULL;
	}
}

std::string BackendSync::Client::stats(){
	std::string s;
	s.append("role:slave");
	s.append("client " + str(link->remote_ip) + ":" + str(link->remote_port) + "\n");
	s.append("    type     : ");
	if(is_mirror){
		s.append("mirror\n");
	}else{
		s.append("sync\n");
	}

	s.append("    status   : ");
	switch(status){
	case INIT:
		s.append("INIT\n");
		break;
	case OUT_OF_SYNC:
		s.append("OUT_OF_SYNC\n");
		break;
	case COPY:
		s.append("COPY\n");
		break;
	case SYNC:
		s.append("SYNC\n");
		break;
	}

	s.append("    last_seq : " + str(last_seq) + "");
	return s;
}

void BackendSync::Client::prepare() {
	std::string start = "";
	start.push_back(DataType::MIN_PREFIX);
	int16_t zero = 0;
	start.append((char*)&zero, sizeof(zero));

	std::string end = "";
	int16_t max = CLUSTER_SLOTS;
	end.append((char*)&max, sizeof(max));

	// for breakpoint
	CopySnapshot *csnapshot = this->backend->last_snapshot(this->host);
	if (csnapshot && csnapshot->snapshot && !this->last_key.empty() && this->last_seq > 0) {
		log_info("start from last breakpoint key(%s)", this->last_key.c_str());
		this->status = COPY;
		this->iter = backend->ssdb->iterator(this->last_key, end, -1, csnapshot->snapshot);

		// skip first key, cause slave has it.
		if (!this->iter->next()) {
			// copy end
			this->status = SYNC;
			this->backend->release_last_snapshot(this->host);
			delete this->iter;
			this->iter = NULL;

			Binlog log(this->last_seq, BinlogType::COPY, BinlogCommand::END, "");
			log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
			link->send(log.repr(), "copy_end");
		}

		return ;
	}

	// start from the very begin
	this->reset();

	// create new snapshot
	csnapshot = this->backend->create_snapshot(this->host);
	assert (csnapshot && csnapshot->snapshot);

	this->iter = backend->ssdb->iterator(start, end, -1, csnapshot->snapshot);
	Binlog log;
	int ret = backend->ssdb->binlogs->find_last(&log, csnapshot->snapshot);
	if(ret == 0) {
		this->last_seq = 0;
	} else {
		this->last_seq = log.seq();
	}
}

void BackendSync::Client::init(){
	const std::vector<Bytes> *req = this->link->last_recv();
	last_seq = 0;
	if(req->size() > 1){
		last_seq = req->at(1).Uint64();
	}
	last_key = "";
	if(req->size() > 2){
		last_key = req->at(2).String();
	}
	if(req->size() > 3){
		this->peer_server_port = req->at(3).Int();
	}
	const char *type = "sync";

	// host
	char buf[32];
	int len = snprintf(buf, 32, "%s:%d", this->link->remote_ip, this->peer_server_port);
	this->host.assign(buf, len);

	// a slave must reset its last_key when receiving 'copy_end' command
	if(last_key == "" && last_seq != 0){
		log_info("[%s] %s:%d fd: %d, sync recover, seq: %" PRIu64 ", key: '%s'",
			type,
			link->remote_ip, link->remote_port,
			link->fd(),
			last_seq, hexmem(last_key.data(), last_key.size()).c_str()
			);
		this->status = Client::SYNC;

		Binlog log(this->last_seq, BinlogType::COPY, BinlogCommand::END, "");
		log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
		link->send(log.repr(), "copy_end");
	}else {
		log_info("[%s] %s:%d fd: %d, copy begin, seq: %" PRIu64 ", key: '%s'",
			type,
			link->remote_ip, link->remote_port,
			link->fd(),
			last_seq, hexmem(last_key.data(), last_key.size()).c_str()
			);
		//this->reset();
        this->prepare();
	}
}

void BackendSync::Client::reset(){
	log_info("%s:%d fd: %d, copy begin", link->remote_ip, link->remote_port, link->fd());
	this->status = Client::COPY;
	this->last_seq = 0;
	this->last_key = "";

	Binlog log(this->last_seq, BinlogType::COPY, BinlogCommand::BEGIN, "");
	log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
	link->send(log.repr(), "copy_begin");
}

void BackendSync::Client::noop(){
	uint64_t seq;
	if(this->status == Client::COPY && this->last_key.empty()){
		seq = 0;
	}else{
		seq = this->last_seq;
		this->last_noop_seq = this->last_seq;
	}
	Binlog noop(seq, BinlogType::NOOP, BinlogCommand::NONE, "");
	//log_debug("fd: %d, %s", link->fd(), noop.dumps().c_str());
	link->send(noop.repr());
}

int BackendSync::Client::copy(){
	int ret = 0;
	int iterate_count = 0;
	int64_t stime = time_ms();

	log_debug("in copy");

	while(true){
		// Prevent copy() from blocking too long
		if(++iterate_count > 1000 || link->output->size() > 2 * 1024 * 1024){
			break;
		}
		if(time_ms() - stime > 3000){
			log_info("copy blocks too long, flush");
			break;
		}

		if(!iter->next()){
			goto copy_end;
		}
		Bytes key = iter->key();
		if(key.size() == 0){
			continue;
		}

		/* encode has changed, deprecated
		// finish copying all valid data types
		if(key.data()[0] > DataType::MAX_PREFIX){
			goto copy_end;
		}
		*/
		if(key.size() > sizeof(int16_t)) {
			int16_t slot = *reinterpret_cast<const int16_t*>(key.data() + key.size() - sizeof(int16_t));
			if(slot >= CLUSTER_SLOTS) {
				log_info("out of cluster slots");
				goto copy_end;
			}
		}

		Bytes val = iter->val();
		this->last_key = key.String();

		char cmd = 0;
		char data_type = key.data()[0];
		if(data_type == DataType::KV){
			cmd = BinlogCommand::KSET;
		}else if(data_type == DataType::HASH){
			cmd = BinlogCommand::HSET;
		}else if(data_type == DataType::ZSET){
			cmd = BinlogCommand::ZSET;
		} else if (data_type == DataType::SET) {
			cmd = BinlogCommand::SSET;
		}else if(data_type == DataType::QUEUE){
			cmd = BinlogCommand::QPUSH_BACK;
			std::string name;
			uint64_t seq = QBACK_SEQ;
			if(decode_qitem_key(key, &name, &seq) == -1 || seq == QBACK_SEQ || seq == QFRONT_SEQ) {
				log_debug("reserve queue item seq %lld, continue", seq);
				continue;
			}
		}else{
			continue;
		}

		ret = 1;

		Binlog log(this->last_seq, BinlogType::COPY, cmd, slice(key));
		log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
		link->send(log.repr(), val);
	}
	return ret;

copy_end:
	log_info("%s:%d fd: %d, copy end", link->remote_ip, link->remote_port, link->fd());
	this->status = Client::SYNC;
	delete this->iter;
	this->iter = NULL;
	backend->release_last_snapshot(this->host);

	Binlog log(this->last_seq, BinlogType::COPY, BinlogCommand::END, "");
	log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
	link->send(log.repr(), "copy_end");
	return 1;
}

int BackendSync::Client::sync(BinlogQueue *logs){
	Binlog log;
	uint64_t expect_seq = this->last_seq + 1;
	int ret = logs->find_next(expect_seq, &log);
	if (ret == 0) {
		return 0;
	}
	if(this->last_seq != 0 && log.seq() != expect_seq){
		log_warn("%s:%d fd: %d, OUT_OF_SYNC! log.seq: %" PRIu64 ", expect_seq: %" PRIu64 "",
			link->remote_ip, link->remote_port,
			link->fd(),
			log.seq(),
			expect_seq
			);
		this->status = Client::OUT_OF_SYNC;
		return 1;
	}
	this->last_seq = log.seq();

	ret = 0;
	std::string val;
	switch(log.cmd()){
		case BinlogCommand::KSET:
		case BinlogCommand::HSET:
		case BinlogCommand::ZSET:
		case BinlogCommand::QSET:
		case BinlogCommand::SSET:
		case BinlogCommand::QPUSH_BACK:
		case BinlogCommand::QPUSH_FRONT:
			ret = backend->ssdb->raw_get(log.key(), &val);
			if(ret == -1){
				log_error("fd: %d, raw_get error!", link->fd());
			}else if(ret == 0){
				//log_debug("%s", hexmem(log.key().data(), log.key().size()).c_str());
				log_trace("fd: %d, skip not found: %s", link->fd(), log.dumps().c_str());
			}else{
				log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
				link->send(log.repr(), val);
			}
			break;
		case BinlogCommand::KDEL:
		case BinlogCommand::HDEL:
		case BinlogCommand::ZDEL:
		case BinlogCommand::SDEL:
		case BinlogCommand::QPOP_BACK:
		case BinlogCommand::QPOP_FRONT:
			log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
			link->send(log.repr());
			break;
	}
	return 1;
}
