/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <pthread.h>
#include <assert.h>
#include <errno.h>
#include <string>
#include "backend_sync2.h"
#include "util/log.h"
#include "util/strings.h"
#include "serv.h"

BackendSync::BackendSync(SSDBServer *owner, SSDBImpl *ssdb, int sync_speed, uint32_t snapshot_timeout){
	thread_quit = false;
	this->owner = owner;
	this->ssdb = ssdb;
	this->sync_speed = sync_speed;

	this->snapshot_timeout = snapshot_timeout;
	pthread_t timer;
	if (pthread_create(&timer, NULL, &BackendSync::_timer_thread, this) != 0) {
		log_error("BackendSync start timer thread failed");
	} else {
		log_info("start backendsync2 timer thread(%lu)", timer);
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

void BackendSync::reset() {
	thread_quit = true;
	while(1) {
		{
			Locking l(&mutex);
			if(workers.empty()) {
				break;
			}
		}
		usleep(50 * 1000);
	}
	thread_quit = false;
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

void* BackendSync::_run_thread(void *arg){
	pthread_detach(pthread_self());
	SET_PROC_NAME("backend_sync2");
	struct run_arg *p = (struct run_arg*)arg;
	BackendSync *backend = (BackendSync *)p->backend;
	Link *link = (Link *)p->link;
	delete p;
	SSDBServer *server = backend->owner;
	int idle = 0;
	struct timeval now;
	struct timespec ts;

	// block IO
	link->noblock(false);

	Client client(backend);
	client.link = link;
	client.init();

	log_info("accept slave(%s:%d)", link->remote_ip, link->remote_port);

	{
		pthread_t tid = pthread_self();
		Locking l(&backend->mutex);
		backend->workers[tid] = &client;
	}

	if (client.status == Client::SYNC)
		goto binlog;

snapshot:
	client.status = Client::COPY;

	// pre snapshot
	if (client.pre_snapshot() != 0) {
		log_error("pre snapshot failed.");
		goto finished;
	}

	log_info("begin transfer snapshot seq(%" PRIu64 ")", client.last_seq);

	// transfer snapshot
	while (!backend->thread_quit) {

		if (client.status == Client::SYNC) {
			log_info("snapshot finished, ready to get binlog.");
			break;
		}

		if (client.copy() <= 0) continue;

		float data_size_mb = link->output->size() / 1024.0 / 1024.0;
		if (link->flush() == -1) {
			log_error("%s:%d, fd(%d) send error.", link->remote_ip,
						link->remote_port, link->fd());

			// mark snapshot abort, for breakpoint
			backend->mark_snapshot(client.host, CopySnapshot::ABORT);
			goto finished;
		}

		if (backend->sync_speed > 0) {
			usleep((data_size_mb/backend->sync_speed) * 1000 * 1000);
		}
	} // end while

	if (backend->thread_quit) {
		goto finished;
	}

	log_info("finished transfer snapshot seq(%" PRIu64 ")", client.last_seq);

	// post snapshot
	client.post_snapshot();

binlog:
	// pre binlog
	if (client.pre_binlog() != 0) {
		log_error("pre binlog failed, reset.");
		client.reset();
		goto snapshot;
	}

	// binlog
	while (!backend->thread_quit) {
		if (client.status != Client::SYNC) {
			log_error("out of sync, reset");
			client.reset();
			goto snapshot;
		}

		LogEvent event;
		unsigned char v;
		int ret = client.read(&event);
		if (ret == 0) { // EOF
			if (!server->binlog->is_active_log(client.logfile.filename)) {
				if (client.next_binlog("") != 0) {
					log_error("walk to next binlog failed. current binlog(%s).",
							client.logfile.filename.c_str());
					goto finished;
				}
				continue;
			}

			pthread_mutex_lock(&server->binlog->mutex);
			gettimeofday(&now, NULL);
			ts.tv_sec = now.tv_sec + 1;
			ts.tv_nsec = now.tv_usec * 1000;
			int err = pthread_cond_timedwait(&server->binlog->cond,
					&server->binlog->mutex, &ts);
			pthread_mutex_unlock(&server->binlog->mutex);

			if (err == ETIMEDOUT) {
				if (++idle > 30) {
					idle = 0;
					client.noop();
					goto flushdata;
				}
			}
			continue;
		}

		idle = 0;

		// binlog error
		if (ret != 1) {
			log_error("read binlog error");
			client.status = Client::OUT_OF_SYNC;
			continue;
		}

		// deal with event
		switch (event.cmd()) {
		case BinlogCommand::ROTATE:
			if (client.next_binlog(std::string(event.key().data(),
							event.key().size())) != 0) {
				log_error("walk to next binlog(%s) failed.", event.key().data());
				goto finished;
			}
			log_info("encounter rotate event. next file (%s)", event.key().data());
			continue;

		case BinlogCommand::STOP:
			if (client.next_binlog("") != 0) {
				log_info("master stopped, and no more binlog");
				goto finished;
			}
			log_info("encounter stop event.");
			continue;

		case BinlogCommand::DESC:
			/* skip description section */
			log_debug("encounter description event.");
			continue;

		default:
			log_debug("backend_sync2 event comand(%" PRIu8 ")", (unsigned char)event.cmd());
			break;
		}

		v = (unsigned char)event.cmd();
		if(v < SSDB_SYNC_CMD_MIN || v > SSDB_SYNC_CMD_MAX) {
			log_debug("skip invalidate event command(%" PRIu8 ")", v);
			continue;
		}

		client.last_seq = event.seq();
		link->send(event.repr());

flushdata:
		if (link->flush() == -1) {
			log_error("%s:%d, fd(%d) send error.", link->remote_ip,
						link->remote_port, link->fd());
			goto finished;
		}
	} // end while

finished:
	log_info("Sync Client quit, %s:%d fd: %d, delete link",
			link->remote_ip, link->remote_port, link->fd());
	delete link;
	client.logfile.close();

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
	s.append("role:slave\n");
	s.append("ip:" + str(link->remote_ip) + "\n");
	s.append("port:"+ str(link->remote_port) + "\n");
	s.append("type:sync\n");

	s.append("status:");
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

	s.append("last_seq:" + str(last_seq));
	return s;
}

void BackendSync::Client::init(){
	const std::vector<Bytes> *req = this->link->last_recv();
	this->last_seq = 0;
	if(req->size() > 1){
		this->last_seq = req->at(1).Uint64();
	}
	this->last_key = "";
	if(req->size() > 2){
		this->last_key = req->at(2).String();
	}
	if (req->size() > 3) {
		this->peer_server_port = req->at(3).Int();
	}
	const char *type = "sync";

	// host
	char buf[32];
	int len = snprintf(buf, 32, "%s:%d", this->link->remote_ip,
			this->peer_server_port);
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

		LogEvent event(this->last_seq, BinlogType::COPY, BinlogCommand::END);
		link->send(event.repr());
	}else if(last_key == "" && last_seq == 0){
		log_info("[%s] %s:%d fd: %d, copy begin, seq: %" PRIu64 ", key: '%s'",
			type,
			link->remote_ip, link->remote_port,
			link->fd(),
			last_seq, hexmem(last_key.data(), last_key.size()).c_str()
			);
		this->status = Client::COPY;
	}else{
		log_info("[%s] %s:%d fd: %d, copy recover, seq: %" PRIu64 ", key: '%s'",
			type,
			link->remote_ip, link->remote_port,
			link->fd(),
			last_seq, hexmem(last_key.data(), last_key.size()).c_str()
			);
		this->status = Client::COPY;
	}
}

void BackendSync::Client::reset(){
	log_info("%s:%d fd: %d, copy begin", link->remote_ip, link->remote_port, link->fd());
	this->status = Client::COPY;
	this->last_seq = 0;
	this->last_key = "";
	this->logreader.unbind();

	LogEvent event(this->last_seq, BinlogType::COPY, BinlogCommand::BEGIN);
	log_trace("fd, %d, %s", link->fd(), event.repr().c_str());
	link->send(event.repr());
}

void BackendSync::Client::noop(){
	uint64_t seq;
	if(this->status == Client::COPY && this->last_key.empty()){
		seq = 0;
	}else{
		seq = this->last_seq;
		this->last_noop_seq = this->last_seq;
	}
	LogEvent noop(seq, BinlogType::NOOP, BinlogCommand::NONE);
	log_debug("send noop to  %s:%d fd: %d", link->remote_ip, link->remote_port, link->fd());
	link->send(noop.repr());
}

int BackendSync::Client::pre_snapshot() {
	assert (this->status == COPY);

	if (this->iter) {
		delete this->iter;
		this->iter = NULL;
	}

	// maximum end
	std::string end;
	int16_t max = CLUSTER_SLOTS;
	end.append((char *)&max, sizeof(max));

	// for breakpoint
	CopySnapshot *csnapshot = this->backend->last_snapshot(this->host);
	if (csnapshot && csnapshot->snapshot && !this->last_key.empty() && this->last_seq > 0) {
		log_info("start from last breakpoint key(%s)", this->last_key.c_str());
		this->status = COPY;
		this->iter = backend->ssdb->iterator(this->last_key, end, UINT_MAX, csnapshot->snapshot);

		// skip first key, cause slave has it.
		if (!this->iter->next()) {
			// copy end
			this->status = SYNC;
			this->backend->release_last_snapshot(this->host);
			delete this->iter;
			this->iter = NULL;

			LogEvent event(this->last_seq, BinlogType::COPY, BinlogCommand::END);
			log_trace("fd: %d, %s", link->fd(), event.repr().c_str());
			link->send(event.repr(), "copy_end");
		}

		log_info("copy from last breakpoint");
		return 0;
	}

	// start from the very begin
	this->reset();

	// create new snapshot
	csnapshot = this->backend->create_snapshot(this->host);
	if (!csnapshot || !csnapshot->snapshot) {
		return -1;
	}

	// construct iterator
	std::string start;
	int16_t slot = -1;
	start.append((char *)&slot, sizeof(slot));
	this->iter = backend->ssdb->iterator(start, end, UINT_MAX, csnapshot->snapshot);
	this->last_key = "";
	this->last_seq = csnapshot->binlog_seq;

	log_info("create snapshot last_seq(%" PRIu64 ")", this->last_seq);

	return 0;
}

int BackendSync::Client::post_snapshot() {
	// release iterator
	if (this->iter) {
		delete this->iter;
		this->iter = NULL;
	}

	// release snapshot
	this->backend->release_last_snapshot(this->host);

	return 0;
}

int BackendSync::Client::pre_binlog() {
	int ret = 0;

	ret = this->seek_binlog(this->last_seq);
	if (ret != 0) {
		log_error("seek binlog for seq %" PRId64 " failed.", this->last_seq);
		return ret;
	}

	return ret;
}

int BackendSync::Client::copy(){
	assert(this->iter);

	int ret = 0;
	int iterate_count = 0;
	int64_t stime = time_ms();
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
		Bytes val = iter->val();
		this->last_key = key.String();
		ret = 1;

		LogEvent event(this->last_seq, BinlogType::COPY, BinlogCommand::RAW, key, val);
		log_trace("fd: %d, key:%s value: %s event:%s", link->fd(),
				hexmem(key.data(), key.size()).c_str(), hexmem(val.data(), val.size()).c_str(),
				hexmem(event.repr().data(), event.repr().size()).c_str());
		link->send(event.repr());
	}

	return ret;

copy_end:
	log_info("%s:%d fd: %d, copy end", link->remote_ip, link->remote_port, link->fd());
	this->status = Client::SYNC;

	delete this->iter;
	this->iter = NULL;

	LogEvent event(this->last_seq, BinlogType::COPY, BinlogCommand::END);
	log_trace("fd: %d, %s", link->fd(), event.repr().c_str());
	link->send(event.repr());

	return 1;
}

int BackendSync::Client::seek_binlog(uint64_t seq) {
	SSDB_BinLog *binlog = backend->owner->binlog;

	// find binlog file
	std::string name = binlog->find_binlog(seq);
	if (name.empty()) { // no such binlog
		log_error("can not find binlog contain seq(%" PRIu64 ").", seq);
		goto out_of_sync;
	}

	binlog->incr_inuse(name);
	this->logfile.filename = binlog->dir() + "/" + name;
	if (this->logfile.open(O_RDONLY, 0644) != 0) {
		log_error("open file(%s) failed, errno(%d).", name.c_str(), errno);
		binlog->decr_inuse(name);
		goto out_of_sync;
	}

	// seek to seq
	this->logreader.bind(&this->logfile);
	if (this->logreader.seek_to_seq(seq) != 0) {
		log_error("file (%s) not contain seq(%" PRIu64 ").", name.c_str(), seq);
		this->logreader.unbind();
		this->logfile.close();
		binlog->decr_inuse(name);
		goto out_of_sync;
	}

	return 0;

out_of_sync:
	log_error("seek to seq (%" PRIu64 ") failed.", seq);
	this->status = OUT_OF_SYNC;
	return -1;
}

int BackendSync::Client::read(LogEvent *event) {
	return this->logreader.read(event);
}

int BackendSync::Client::next_binlog(const std::string &nextfile) {
	SSDB_BinLog *binlog = this->backend->owner->binlog;
	std::string binlog_dir = binlog->dir();

	// release current binlog
	std::string current = this->logfile.filename;
	size_t pos = current.rfind('/');
	if (pos == std::string::npos) {
		log_error("filename invalid.");
		return -1;
	}
	std::string current_name(current.c_str() + pos + 1, current.size() - pos - 1);
	this->logreader.unbind();
	this->logfile.close();
	binlog->decr_inuse(current_name);

	// jump to next binlog
	if (nextfile.empty()) {
		std::string next = binlog->find_next(current_name);
		if (next.empty()) {
			log_error("no binlog after behind (%s).", current_name.c_str());
			return -1;
		}

		binlog->incr_inuse(next);
		this->logfile.filename = binlog_dir + "/" + next;
	} else {
		binlog->incr_inuse(nextfile);
		this->logfile.filename = binlog_dir + "/" + nextfile;
	}

	if (this->logfile.open(O_RDONLY, 0644) != 0) {
		log_error("open file(%s) for read failed, errno(%d).",
				this->logfile.filename.c_str(), errno);
		return -1;
	}
	this->logreader.bind(&this->logfile);

	return 0;
}

/* timer snapshot */

void* BackendSync::_timer_thread(void *arg) {
	pthread_detach(pthread_self());
	SET_PROC_NAME("backend_sync2_timer");
	BackendSync *sync = (BackendSync *)arg;

	pthread_cond_t cond;
	pthread_mutex_t mutex;
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_lock(&mutex);
	while (!sync->thread_quit) {
		struct timespec timeout;
		timeout.tv_sec = time(NULL) + 30;
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
	return NULL;
}

void BackendSync::clear_timeout_snapshot() {
	Locking l(&mutex);

	if (snapshots.empty()) return;

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

	for (size_t i = 0; i < purges.size(); i++) {
		log_info("clean snapshot for host(%s)", purges[i].c_str());
		it = snapshots.find(purges[i]);
		assert (it != snapshots.end());

		csnapshot = it->second;
		if (csnapshot && csnapshot->snapshot) {
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

#define CLEAR_LINK(link) \
do { \
	link->close(); \
	delete link; \
} while(0)

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

	SSDBServer *server = this->owner;

	// lockdb gurantee the atomicly of create leveldb snapshot and get binlog seq.
	server->ssdb->lock_db();

	const leveldb::Snapshot *snapshot = server->ssdb->get_snapshot();
	uint64_t binlog_last_seq = server->binlog->get_last_seq();
	time_t now = time(NULL);

	CopySnapshot *csnapshot = new CopySnapshot;
	csnapshot->snapshot = snapshot;
	csnapshot->status = CopySnapshot::ACTIVE;
	csnapshot->last_active = now;
	csnapshot->binlog_seq = binlog_last_seq;
	snapshots.insert(std::make_pair<std::string, CopySnapshot *>(host, csnapshot));

	server->ssdb->unlock_db();

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
