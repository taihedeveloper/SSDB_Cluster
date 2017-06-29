#ifndef SSDB_RANGE_MIGRATE_H_
#define SSDB_RANGE_MIGRATE_H_

#include "include.h"
#include "leveldb/slice.h"
#include "net/fde.h"
#include "ssdb/ssdb.h"
#include "ssdb/ssdb_impl.h"
#include "ssdb/binlog2.h"
#include "ssdb/ttl.h"

class Link;
class Response;
class SegKeyLock;
class RangeMigrate {
public:
	RangeMigrate(SSDB_BinLog *binlog, SSDB *ssdb, ExpirationHandler *expiration, SegKeyLock &lock);
	~RangeMigrate();

	void migrate(Link *link, Response *resp, const std::string &ip, int port,
		const std::string &start, const std::string &end, int speed, int64_t timeout_ms);
	void import(Link *link, const std::string &prefix);

	int get_migrate_ret();                        /* get last migrate status */
	std::string get_migrate_msg(int n);           /* get error msg by errno */

private:
	SSDB_BinLog *binlog;
	SSDB *ssdb;
	ExpirationHandler *expiration;
	SegKeyLock &key_lock;
	int migrate_ret;  /* 0: done 1: processing -1: error(abort) */
	int processing:1; /* only one migrate thread */
	static void *_migrate_thread(void *arg);
	static void *_import_thread(void *arg);

	struct _thread_args {
		RangeMigrate *owner;
		std::string ip;
		int port;
		std::string start;
		std::string end;
		std::string sync_key;
		Link *link;
		Link *upstream;
		Response *resp;
		int speed;
		int64_t timeout;
		bool clear_key;
	};

	class Client {
	public:
		Client();
		~Client();
		void proc();                           /* main process */
		void init();                           /* send 'migrate' command and adjust iterator */
		void copy();                           /* send data to the target */
		void confirm(char cmd);                /* read a command */
		void confirm_ack();                    /* read ack and delete key*/
		void confirm_eof();                    /* read end, delete key and call cb */
		int flush();                           /* flush data to network */
		void connect();                        /* connect to the target 'ip:port' */
		void reconnect();                      /* reset link, iterator and status */
		int key_migrate_init();                /* find and add prefix to key-lock-set */
		int adjust(const LogEvent &log);       /* adjust the args of the key specified */
		int copy_kv();
		int copy_hash();
		int copy_set();
		int copy_zset();
		int copy_queue();

		enum {
			CLIENT_DISCONNECT = 0,
			CLIENT_CONNECTED,
			CLIENT_RECONNECT,
			CLIENT_INITIALIZED,
			CLIENT_COPY,
			CLIENT_ACK,
			CLIENT_EOF,
			CLIENT_ABORT,
			CLIENT_DONE,
		} status;                 /* sync stauts */

		RangeMigrate *owner;      /* owner of this client */
		std::string ip;	          /* remote ip */
		int port;                 /* remote port */
		std::string start;        /* range start */
		std::string end;          /* range end */
		Link *link;               /* link for sync */
		Link *upstream;           /* link for return */
		int sync_speed;           /* max speed for sync */
		Fdevents select;          /* event pool for io */
		std::string sync_key;     /* user key expected */
		char sync_type;		      /* opration type */
		uint64_t sync_version;    /* version of sync key */
		int64_t sync_qcount;      /* arg 'seq' for queue */
		int64_t ttl;              /* arg 'ttl' */
		std::string sync_field;   /* arg 'field' for zset, set and hash */
		uint64_t send_count;      /* number of raw key sended */
		int64_t proc_timeout;     /* migrating timeout(ms) */
		int64_t proc_start;       /* migrating start time */
		Response *resp;           /* response vector */
	};

	class Server {
	public:
		Server();
		~Server();

		RangeMigrate *owner;     /* owner of this server */
		Link *link;              /* link fo sync */
		std::string sync_key;    /* current sync key */
		uint64_t sync_version;   /* version for sync key */
		char sync_type;          /* data type for sync key */ 
		Fdevents select;         /* event pool for io */
		int copy_count;          /* number of raw key copied */
		bool first;              /* flag of first raw key */

		void proc();                                   /* main process */
		int init();                                    /* response to 'migrate' */
		int init_meta(Transaction &trans);             /* init version and type */
		int proc_req(const std::vector<Bytes> &req);   /* process the requests */
		int send_cmd(char log_type, char cmd,
			const Bytes &key, const Bytes &value); /* send a command */
	};
};

#endif
