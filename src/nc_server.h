/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _NC_SERVER_H_
#define _NC_SERVER_H_

#include <nc_core.h>
#include <nc_zookeeper.h>

/*
 * server_pool is a collection of servers and their continuum. Each
 * server_pool is the owner of a single proxy connection and one or
 * more client connections. server_pool itself is owned by the current
 * context.
 *
 * Each server is the owner of one or more server connections. server
 * itself is owned by the server_pool.
 *
 *  +-------------+
 *  |             |<---------------------+
 *  |             |<------------+        |
 *  |             |     +-------+--+-----+----+--------------+
 *  |   pool 0    |+--->|          |          |              |
 *  |             |     | server 0 | server 1 | ...     ...  |
 *  |             |     |          |          |              |--+
 *  |             |     +----------+----------+--------------+  |
 *  +-------------+                                             //
 *  |             |
 *  |             |
 *  |             |
 *  |   pool 1    |
 *  |             |
 *  |             |
 *  |             |
 *  +-------------+
 *  |             |
 *  |             |
 *  .             .
 *  .    ...      .
 *  .             .
 *  |             |
 *  |             |
 *  +-------------+
 *            |
 *            |
 *            //
 */

#define HASHSLOT_SLOT_NUM             16384
typedef uint32_t (*hash_t)(const char *, size_t);

struct continuum {
    uint32_t index;  /* server index */
    uint32_t value;  /* hash value */
};

struct server {
    uint32_t           idx;           /* server index */
    struct server_pool *owner;        /* owner pool */

    struct string      pname;         /* hostname:port:weight (ref in conf_server) */
    struct string      name;          /* hostname:port or [name] (ref in conf_server) */
    struct string      addrstr;       /* hostname (ref in conf_server) */
    uint16_t           port;          /* port */
    uint32_t           weight;        /* weight */
    struct sockinfo    info;          /* server socket info */

    uint32_t           ns_conn_q;     /* # server connection */
    struct conn_tqh    s_conn_q;      /* server connection q */

    int64_t            next_retry;    /* next retry time in usec */
    uint32_t           failure_count; /* # consecutive failures */
	uint32_t           connected;
	uint32_t           is_read;
	struct server_group* group;
};

struct server_group
{
	struct array servers;
	uint32_t loop_flag;
	uint32_t current_index;
};

struct slot_ctx
{
    int slot_index;
    struct server_pool *pool;
};

struct server_pool {
    uint32_t           idx;                  /* pool index */
    struct context     *ctx;                 /* owner context */

    struct conn        *p_conn;              /* proxy connection (listener) */
    uint32_t           nc_conn_q;            /* # client connection */
    struct conn_tqh    c_conn_q;             /* client connection q */

    struct array       server_group;
    struct array       server;               /* server[] */
	struct array       write_server;
    struct array       backup_server;        /* backup server []*/
    struct array       zookeeper_server;        /* backup server []*/
    uint32_t           ncontinuum;           /* # continuum points */
    uint32_t           nserver_continuum;    /* # servers - live and dead on continuum (const) */
    struct continuum   *continuum;           /* continuum */
    uint32_t           nhashslotnum;          /*hash slot number*/
    struct continuum   *hashslot;            /* hashslot */
    uint32_t           nlive_server;         /* # live server */
    int64_t            next_rebuild;         /* next distribution rebuild time in usec */

    struct string      name;                 /* pool name (ref in conf_pool) */
    struct string      addrstr;              /* pool address - hostname:port (ref in conf_pool) */
    uint16_t           port;                 /* port */
    struct sockinfo    info;                 /* listen socket info */
    mode_t             perm;                 /* socket permission */
    int                dist_type;            /* distribution type (dist_type_t) */
    int                key_hash_type;        /* key hash type (hash_type_t) */
    hash_t             key_hash;             /* key hasher */
    struct string      hash_tag;             /* key hash tag (ref in conf_pool) */
    int                timeout;              /* timeout in msec */
    int                backlog;              /* listen backlog */
    int                redis_db;             /* redis database to connect to */
    uint32_t           client_connections;   /* maximum # client connection */
    uint32_t           server_connections;   /* maximum # server connection */
    int64_t            server_retry_timeout; /* server retry timeout in usec */
    uint32_t           server_failure_limit; /* server failure limit */
	uint32_t           protocol;
    struct string      redis_auth;           /* redis_auth password (matches requirepass on redis) */
    unsigned           require_auth;         /* require_auth? */
    unsigned           auto_eject_hosts:1;   /* auto_eject_hosts? */
    unsigned           preconnect:1;         /* preconnect? */
    unsigned           master:1;             /* master? */
    unsigned           tcpkeepalive:1;       /* tcpkeepalive? */
    unsigned           finish_init:1;        /* finish init */
    struct array       ctx_array;            /* slot_ctx */
    zhandle_t          *zh_handler;          /* zookeeper handler */
    struct array       server_identifier;     /* server_identified */
    struct zk_init_ctx *init_ctx;            /* zookeeper init watcher ctx*/
    void               *ssdb_handle;          /*ssdb handle*/
    uint64_t           last_seq;
};

void server_ref(struct conn *conn, void *owner);
void server_unref(struct conn *conn);
int server_timeout(struct conn *conn);
bool server_active(struct conn *conn);
rstatus_t server_group_init(struct array *server_group, struct array *conf_server_group, struct server_pool *sp);
rstatus_t server_init(struct array *server, struct array *conf_server, struct server_pool *sp);
rstatus_t write_server_init(struct array *server, struct array *conf_server, struct server_pool *sp);
rstatus_t backup_server_init(struct array *server, struct array *conf_server, struct server_pool *sp);
rstatus_t server_identifier_init(struct array *server, struct array *back_server, struct server_pool *sp);
rstatus_t slotmap_ctx_init(struct array *ctx_array);
rstatus_t server_update_index(struct array *ar , uint32_t index);
void server_deinit(struct array *server);
struct conn *server_conn(struct server *server);
rstatus_t server_connect(struct context *ctx, struct server *server, struct conn *conn);
rstatus_t server_connect_determine(struct context *ctx, struct server *server, struct conn *conn);
void server_close(struct context *ctx, struct conn *conn);
void server_connected(struct context *ctx, struct conn *conn);
void server_ok(struct context *ctx, struct conn *conn);

uint32_t server_pool_idx(struct server_pool *pool, uint8_t *key, uint32_t keylen);
struct conn *server_pool_conn(struct context *ctx, struct server_pool *pool, uint8_t *key, uint32_t keylen, uint32_t write);
rstatus_t server_pool_run(struct server_pool *pool);
rstatus_t server_pool_preconnect(struct context *ctx);
void server_pool_disconnect(struct context *ctx);
rstatus_t server_pool_connected_determine(struct context *ctx);
rstatus_t server_pool_init(struct array *server_pool, struct array *conf_pool, struct context *ctx);
void server_pool_deinit(struct array *server_pool);
rstatus_t server_active_standby_switch(struct server *server);

#endif
