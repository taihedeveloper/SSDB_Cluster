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

#include <stdlib.h>
#include <unistd.h>
#include <sys/select.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_conf.h>

static void
server_resolve(struct server *server, struct conn *conn)
{
    rstatus_t status;

    status = nc_resolve(&server->addrstr, server->port, &server->info);
    if (status != NC_OK) {
        conn->err = EHOSTDOWN;
        conn->done = 1;
        return;
    }

    conn->family = server->info.family;
    conn->addrlen = server->info.addrlen;
    conn->addr = (struct sockaddr *)&server->info.addr;
}

void
server_ref(struct conn *conn, void *owner)
{
    struct server *server = owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->owner == NULL);

    server_resolve(server, conn);

    server->ns_conn_q++;
    TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

    conn->owner = owner;

    log_debug(LOG_VVERB, "ref conn %p owner %p into '%.*s", conn, server,
              server->pname.len, server->pname.data);
}

void
server_unref(struct conn *conn)
{
    struct server *server;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->owner != NULL);

    server = conn->owner;
    conn->owner = NULL;

    ASSERT(server->ns_conn_q != 0);
    server->ns_conn_q--;
    TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);

    log_debug(LOG_VVERB, "unref conn %p owner %p from '%.*s'", conn, server,
              server->pname.len, server->pname.data);
}

int
server_timeout(struct conn *conn)
{
    struct server *server;
    struct server_pool *pool;

    ASSERT(!conn->client && !conn->proxy);

    server = conn->owner;
    pool = server->owner;

    return pool->timeout;
}

bool
server_active(struct conn *conn)
{
    ASSERT(!conn->client && !conn->proxy);

    if (!TAILQ_EMPTY(&conn->imsg_q)) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    log_debug(LOG_VVERB, "s %d is inactive", conn->sd);

    return false;
}

static rstatus_t
server_each_set_owner(void *elem, void *data)
{
    struct server *s = elem;
    struct server_pool *sp = data;

    s->owner = sp;

    return NC_OK;
}

static rstatus_t
server_each_set_group(void *elem, void *data)
{
    struct server *s = elem;
    s->group = data;

    return NC_OK;
}

rstatus_t server_group_init(struct array *server_group, struct array *conf_server_group, struct server_pool *sp)
{
	rstatus_t status;
    uint32_t nserver_group;
	uint32_t i;
	struct conf_server_group* conf_group;
	struct server_group* sgroup;

    nserver_group = array_n(conf_server_group);
    ASSERT(nserver != 0);
    ASSERT(array_n(server) == 0);

    status = array_init(server_group, nserver_group, sizeof(struct server_group));
    if (status != NC_OK) {
        return status;
    }
	sgroup     = (struct server_group*)server_group->elem;
	conf_group = (struct conf_server_group*)conf_server_group->elem;
	
	for (i = 0; i < conf_server_group->nelem; i++)
	{
		status = array_init(&sgroup->servers, array_n(&conf_group->server), sizeof(struct server));
		if (status != NC_OK) {
			return status;
		}
		status = array_each(&conf_group->server, conf_server_each_transform, &sgroup->servers);
		if (status != NC_OK) {
			return status;
		}
		
		status = array_each(&sgroup->servers, server_each_set_owner, sp);
		if (status != NC_OK) {
			return status;
		}
		
		status = array_each(&sgroup->servers, server_each_set_group, sgroup);
		if (status != NC_OK) {
			return status;
		}
		sgroup->loop_flag = (uint32_t)conf_group->loop;
		sgroup->current_index = 0;
		conf_group++;
		sgroup++;
	}

    return NC_OK;
}

rstatus_t
server_init(struct array *server, struct array *conf_server,
            struct server_pool *sp)
{
    rstatus_t status;
    uint32_t nserver;

    nserver = array_n(conf_server);
    ASSERT(nserver != 0);
    ASSERT(array_n(server) == 0);

    status = array_init(server, nserver * 5, sizeof(struct server));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf server to server */
    status = array_each(conf_server, conf_server_each_transform, server);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }
    ASSERT(array_n(server) == nserver);

    /* set server owner */
    status = array_each(server, server_each_set_owner, sp);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }

    log_debug(LOG_DEBUG, "init %"PRIu32" servers in pool %"PRIu32" '%.*s'",
              nserver, sp->idx, sp->name.len, sp->name.data);

    return NC_OK;
}


rstatus_t
write_server_init(struct array *server, struct array *conf_server,
            struct server_pool *sp)
{
    rstatus_t status;
    uint32_t nserver;

    nserver = array_n(conf_server);
    ASSERT(nserver != 0);
    ASSERT(array_n(server) == 0);

    status = array_init(server, nserver * 5, sizeof(struct server));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf server to server */
    status = array_each(conf_server, conf_server_each_transform, server);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }

    /* set server owner */
    status = array_each(server, server_each_set_owner, sp);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }

    log_debug(LOG_DEBUG, "init %"PRIu32" servers in pool %"PRIu32" '%.*s'",
              nserver, sp->idx, sp->name.len, sp->name.data);

    return NC_OK;
}

rstatus_t
backup_server_init(struct array *server, struct array *conf_server,
            struct server_pool *sp)
{
    rstatus_t status;
    uint32_t nserver;

    nserver = array_n(conf_server);
    ASSERT(nserver != 0);
    ASSERT(array_n(server) == 0);

    status = array_init(server, nserver * 5, sizeof(struct server));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf server to server */
    status = array_each(conf_server, conf_server_each_transform, server);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }

    /* set server owner */
    status = array_each(server, server_each_set_owner, sp);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }

    log_debug(LOG_DEBUG, "init %"PRIu32" servers in pool %"PRIu32" '%.*s'",
              nserver, sp->idx, sp->name.len, sp->name.data);

    return NC_OK;
}

rstatus_t
server_identifier_init(struct array *server, struct array *back_server,
            struct server_pool *sp)
{
    uint32_t nserver;
    uint32_t nbackserver;

    uint32_t server_index;
    uint32_t *crc;
    struct server *s, *bs;

    nserver = array_n(back_server);
    ASSERT(nserver != 0);
    ASSERT(nbackserver != 0);
    ASSERT(nbackserver == nserver);

    array_init(&sp->server_identifier, nserver * 5, sizeof(uint32_t));

    for(server_index = 0; server_index < nserver; server_index++) {
        s  = array_get(server, server_index);
        bs = array_get(back_server, server_index);
        crc = (uint32_t *)array_push(&sp->server_identifier);
        *crc = (hash_crc16((const char *)s->pname.data, s->pname.len) << 16) +
                hash_crc16((const char *)bs->pname.data, bs->pname.len);
    }

    log_debug(LOG_DEBUG, "init %"PRIu32" server_identifier in pool %"PRIu32" '%.*s'",
              nserver, sp->idx, sp->name.len, sp->name.data);

    return NC_OK;
}

rstatus_t
slotmap_ctx_init(struct array *ctx_array)
{
    rstatus_t status;

    status = array_init(ctx_array, HASHSLOT_SLOT_NUM, sizeof(struct slot_ctx));
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

void
server_deinit(struct array *server)
{
    uint32_t i, nserver;

    for (i = 0, nserver = array_n(server); i < nserver; i++) {
        struct server *s;

        s = array_pop(server);
        ASSERT(TAILQ_EMPTY(&s->s_conn_q) && s->ns_conn_q == 0);
    }
    array_deinit(server);
}

struct conn *
server_conn(struct server *server)
{
    struct server_pool *pool;
    struct conn *conn;

    pool = server->owner;

    /*
     * FIXME: handle multiple server connections per server and do load
     * balancing on it. Support multiple algorithms for
     * 'server_connections:' > 0 key
     */

    if (server->ns_conn_q < pool->server_connections) {
        return conn_get(server, false, (int)pool->protocol);
    }
    ASSERT(server->ns_conn_q == pool->server_connections);

    /*
     * Pick a server connection from the head of the queue and insert
     * it back into the tail of queue to maintain the lru order
     */
    conn = TAILQ_FIRST(&server->s_conn_q);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);
    TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

    return conn;
}

static rstatus_t
server_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server *server;
    struct server_pool *pool;
    struct conn *conn;

    server = elem;
    pool = server->owner;

    conn = server_conn(server);
    if (conn == NULL) {
        return NC_ENOMEM;
    }

    status = server_connect(pool->ctx, server, conn);
    if (status != NC_OK) {
        log_warn("connect to server '%.*s' failed, ignored: %s",
                 server->pname.len, server->pname.data, strerror(errno));
        server_close(pool->ctx, conn);
    }

    return NC_OK;
}

static rstatus_t
server_each_disconnect(void *elem, void *data)
{
    struct server *server;
    struct server_pool *pool;

    server = elem;
    pool = server->owner;

    while (!TAILQ_EMPTY(&server->s_conn_q)) {
        struct conn *conn;

        ASSERT(server->ns_conn_q > 0);

        conn = TAILQ_FIRST(&server->s_conn_q);
        conn->close(pool->ctx, conn);
    }

    return NC_OK;
}

static rstatus_t
server_each_connected_determine(void *elem, void *data)
{
    rstatus_t status;
    struct server *server, *master_server;
    struct server_pool *pool;
    struct conn *conn;
    uint32_t index;
    int64_t now;                  /* current timestamp in usec */

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    server = elem;
    pool = server->owner;

    if(server->next_retry > now) {
        index = array_idx(&pool->backup_server, server);
        master_server = array_get(&pool->server, index);

        if (server->ns_conn_q < pool->server_connections) {
            conn =  conn_get(server, false, (int)pool->protocol);
            if (conn == NULL) {
                return NC_ENOMEM;
            }
        }else{
            conn = TAILQ_FIRST(&server->s_conn_q);
        }

        conn->err = 0;
        conn->sd  = -1;
        status = server_connect_determine(pool->ctx, server, conn);
        if (status != NC_OK) {
            log_warn("connect to server '%.*s' failed, ignored: %s, pid:%lu, status:%d",
                     server->pname.len, server->pname.data, strerror(errno), (uint64_t)pthread_self(), status);
            server->next_retry = now + pool->server_retry_timeout;
        }else {
            server->next_retry = 0LL;
            log_warn("connect to server '%.*s' success",
                     server->pname.len, server->pname.data);
            if(pool->master)
            {
                if(pool->ssdb_handle) {
                    lib_ssdb_change_master_to_t change_master_to = (lib_ssdb_active_standby_switch_t)dlsym(pool->ssdb_handle, "change_master_to");
                    int err_no = change_master_to((const char*)server->addrstr.data, server->port, pool->last_seq,
                                        (const char *)master_server->addrstr.data, master_server->port);
                }else{
                    log_warn("ssdb handle is NULL");
                    return NC_ERROR;
                }
            }

            conn->unref(conn);
            conn_put(conn);
//
//            if(conn->sd > 0)
//            {
//                log_warn("put");
//                close(conn->sd);
//                conn->unref(conn);
//                conn_put(conn);
//            }else{
//                log_warn("no put");
//            }
        }
    }
    return NC_OK;
}

static void
server_failure(struct context *ctx, struct server *server)
{
    struct server_pool *pool = server->owner;
    int64_t now, next;
    rstatus_t status;

    if (!pool->auto_eject_hosts) {
        return;
    }
	
	server->connected = 0;
	
	if (server->is_read)
	{
		
	}

    server->failure_count++;

    log_debug(LOG_VERB, "server '%.*s' failure count %"PRIu32" limit %"PRIu32,
              server->pname.len, server->pname.data, server->failure_count,
              pool->server_failure_limit);

    if (server->failure_count < pool->server_failure_limit) {
        return;
    }

    now = nc_usec_now();
    if (now < 0) {
        return;
    }

    stats_server_set_ts(ctx, server, server_ejected_at, now);

    next = now + pool->server_retry_timeout;

    log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to delete server '%.*s' "
              "for next %"PRIu32" secs", pool->idx, pool->name.len,
              pool->name.data, server->pname.len, server->pname.data,
              pool->server_retry_timeout / 1000 / 1000);

    stats_pool_incr(ctx, pool, server_ejects);

    server->failure_count = 0;
    server->next_retry = next;

    status = server_pool_run(pool);
    if (status != NC_OK) {
        log_error("updating pool %"PRIu32" '%.*s' failed: %s", pool->idx,
                  pool->name.len, pool->name.data, strerror(errno));
    }
}

static void
server_close_stats(struct context *ctx, struct server *server, err_t err,
                   unsigned eof, unsigned connected)
{
    if (connected) {
        stats_server_decr(ctx, server, server_connections);
    }

    if (eof) {
        stats_server_incr(ctx, server, server_eof);
        return;
    }

    switch (err) {
    case ETIMEDOUT:
        stats_server_incr(ctx, server, server_timedout);
        break;
    case EPIPE:
    case ECONNRESET:
    case ECONNABORTED:
    case ECONNREFUSED:
    case ENOTCONN:
    case ENETDOWN:
    case ENETUNREACH:
    case EHOSTDOWN:
    case EHOSTUNREACH:
    default:
        stats_server_incr(ctx, server, server_err);
        break;
    }
}

void
server_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */
    struct conn *c_conn;    /* peer client connection */

    ASSERT(!conn->client && !conn->proxy);

    server_close_stats(ctx, conn->owner, conn->err, conn->eof,
                       conn->connected);

    conn->connected = false;

    if (conn->sd < 0) {
        server_failure(ctx, conn->owner);
        conn->unref(conn);
        conn_put(conn);
        return;
    }

    for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from server inq */
        conn->dequeue_inq(ctx, conn, msg);

        /*
         * Don't send any error response, if
         * 1. request is tagged as noreply or,
         * 2. client has already closed its connection
         */
        if (msg->swallow || msg->noreply) {
            log_debug(LOG_INFO, "close s %d swallow req %"PRIu64" len %"PRIu32
                      " type %d", conn->sd, msg->id, msg->mlen, msg->type);
            req_put(msg);
        } else {
            c_conn = msg->owner;
            ASSERT(c_conn->client && !c_conn->proxy);

            msg->done = 1;
            msg->error = 1;
            msg->err = conn->err;

            if (msg->frag_owner != NULL) {
                msg->frag_owner->nfrag_done++;
            }

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
                event_add_out(ctx->evb, msg->owner);
            }

            log_debug(LOG_INFO, "close s %d schedule error for req %"PRIu64" "
                      "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
                      msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
                      conn->err ? strerror(conn->err): " ");
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from server outq */
        conn->dequeue_outq(ctx, conn, msg);

        if (msg->swallow) {
            log_debug(LOG_INFO, "close s %d swallow req %"PRIu64" len %"PRIu32
                      " type %d", conn->sd, msg->id, msg->mlen, msg->type);
            req_put(msg);
        } else {
            c_conn = msg->owner;
            ASSERT(c_conn->client && !c_conn->proxy);

            msg->done = 1;
            msg->error = 1;
            msg->err = conn->err;
            if (msg->frag_owner != NULL) {
                msg->frag_owner->nfrag_done++;
            }

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
                event_add_out(ctx->evb, msg->owner);
            }

            log_debug(LOG_INFO, "close s %d schedule error for req %"PRIu64" "
                      "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
                      msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
                      conn->err ? strerror(conn->err): " ");
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        ASSERT(!msg->request);
        ASSERT(msg->peer == NULL);

        rsp_put(msg);

        log_debug(LOG_INFO, "close s %d discarding rsp %"PRIu64" len %"PRIu32" "
                  "in error", conn->sd, msg->id, msg->mlen);
    }

    ASSERT(conn->smsg == NULL);

    server_failure(ctx, conn->owner);

    struct server * s = (struct server*)conn->owner;

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close s %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);

    if(!s->ns_conn_q){
        server_active_standby_switch(s);
    }
}

rstatus_t
server_connect(struct context *ctx, struct server *server, struct conn *conn)
{
    rstatus_t status;

    ASSERT(!conn->client && !conn->proxy);

    if (conn->err) {
      ASSERT(conn->done && conn->sd < 0);
      errno = conn->err;
      return NC_ERROR;
    }

    if (conn->sd > 0) {
        /* already connected on server connection */
        return NC_OK;
    }

    log_debug(LOG_VVERB, "connect to server '%.*s'", server->pname.len,
              server->pname.data);

    conn->sd = socket(conn->family, SOCK_STREAM, 0);
    if (conn->sd < 0) {
        log_error("socket for server '%.*s' failed: %s", server->pname.len,
                  server->pname.data, strerror(errno));
        status = NC_ERROR;
        goto error;
    }

    status = nc_set_nonblocking(conn->sd);
    if (status != NC_OK) {
        log_error("set nonblock on s %d for server '%.*s' failed: %s",
                  conn->sd, server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    if (server->pname.data[0] != '/') {
        status = nc_set_tcpnodelay(conn->sd);
        if (status != NC_OK) {
            log_warn("set tcpnodelay on s %d for server '%.*s' failed, ignored: %s",
                     conn->sd, server->pname.len, server->pname.data,
                     strerror(errno));
        }
    }

    status = event_add_conn(ctx->evb, conn);
    if (status != NC_OK) {
        log_error("event add conn s %d for server '%.*s' failed: %s",
                  conn->sd, server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    ASSERT(!conn->connecting && !conn->connected);

    status = connect(conn->sd, conn->addr, conn->addrlen);
    if (status != NC_OK) {
        if (errno == EINPROGRESS) {
            conn->connecting = 1;
            log_debug(LOG_DEBUG, "connecting on s %d to server '%.*s'",
                      conn->sd, server->pname.len, server->pname.data);
            return NC_OK;
        }

        log_error("connect on s %d to server '%.*s' failed: %s", conn->sd,
                  server->pname.len, server->pname.data, strerror(errno));

        goto error;
    }

    ASSERT(!conn->connecting);
    conn->connected = 1;
    log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
              server->pname.len, server->pname.data);

    return NC_OK;

error:
    conn->err = errno;
    return status;
}


rstatus_t
server_connect_determine(struct context *ctx, struct server *server, struct conn *conn)
{
    rstatus_t status;
    fd_set  readfds, writefds, exfds;
    struct timeval tv;
    socklen_t len;

    int fd = 0;

    ASSERT(!conn->client && !conn->proxy);

    if (conn->err) {
      ASSERT(conn->done && conn->sd < 0);
      errno = conn->err;
      return NC_ERROR;
    }

//    if (conn->sd > 0) {
//        /* already connected on server connection */
//        return NC_OK;
//    }

    log_debug(LOG_VVERB, "connect to server '%.*s'", server->pname.len,
              server->pname.data);

    //conn->sd = socket(conn->family, SOCK_STREAM, 0);
    fd = socket(conn->family, SOCK_STREAM, 0);

    if (fd < 0) {
        log_error("socket for server '%.*s' failed: %s", server->pname.len,
                  server->pname.data, strerror(errno));
        status = NC_ERROR;
        goto error;
    }

    status = nc_set_nonblocking(fd);
    if (status != NC_OK) {
        log_error("set nonblock on s %d for server '%.*s' failed: %s",
                  conn->sd, server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    if (server->pname.data[0] != '/') {
        status = nc_set_tcpnodelay(fd);
        if (status != NC_OK) {
            log_warn("set tcpnodelay on s %d for server '%.*s' failed, ignored: %s",
                     conn->sd, server->pname.len, server->pname.data,
                     strerror(errno));
        }
    }


//    status = event_add_conn(ctx->evb, conn);
//    if (status != NC_OK) {
//        log_error("event add conn s %d for server '%.*s' failed: %s",
//                  conn->sd, server->pname.len, server->pname.data,
//                  strerror(errno));
//        goto error;
//    }


    ASSERT(!conn->connecting && !conn->connected);
    status = connect(fd, conn->addr, conn->addrlen);
    if (status != NC_OK) {
        if (errno == EINPROGRESS) {
//            conn->connecting = 1;
//            log_debug(LOG_DEBUG, "connecting on s %d to server '%.*s'",
//                      conn->sd, server->pname.len, server->pname.data);
            FD_ZERO(&readfds);
            FD_ZERO(&writefds);
            FD_ZERO(&exfds);
            FD_SET(fd, &readfds);
            FD_SET(fd, &writefds);
            FD_SET(fd, &exfds);
            tv.tv_sec  = 1;
            tv.tv_usec = 0;
            int ret = select(fd + 1, &readfds, &writefds, &exfds, &tv);
            if(ret <= 0){
                log_error("connect on s %d to server '%.*s' failed: %s", conn->sd,
                          server->pname.len, server->pname.data, strerror(errno));
                status = NC_ERROR;
                goto error;
            }else{
                status = getpeername(fd, conn->addr, &conn->addrlen);
                if (status != NC_OK) {
                    goto error;
                }
                return NC_OK;
            }
        }

        log_error("connect on s %d to server '%.*s' failed: %s", conn->sd,
                  server->pname.len, server->pname.data, strerror(errno));

        goto error;
    }

    ASSERT(!conn->connecting);
    conn->connected = 1;
    log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
              server->pname.len, server->pname.data);
    close(fd);
    return NC_OK;

error:
    close(fd);
    conn->err = errno;
    return status;
}

void
server_connected(struct context *ctx, struct conn *conn)
{
    struct server *server = conn->owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->connecting && !conn->connected);

    stats_server_incr(ctx, server, server_connections);

    conn->connecting = 0;
    conn->connected = 1;

    conn->post_connect(ctx, conn, server);

    log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
              server->pname.len, server->pname.data);
}

void
server_ok(struct context *ctx, struct conn *conn)
{
    struct server *server = conn->owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->connected);

    if (server->failure_count != 0) {
        log_debug(LOG_VERB, "reset server '%.*s' failure count from %"PRIu32
                  " to 0", server->pname.len, server->pname.data,
                  server->failure_count);
        server->failure_count = 0;
        server->next_retry = 0LL;
    }
}

static rstatus_t
server_pool_update(struct server_pool *pool)
{
    rstatus_t status;
    int64_t now;
    uint32_t pnlive_server; /* prev # live server */

    if (!pool->auto_eject_hosts) {
        return NC_OK;
    }

    if (pool->next_rebuild == 0LL) {
        return NC_OK;
    }

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    if (now <= pool->next_rebuild) {
        if (pool->nlive_server == 0) {
            errno = ECONNREFUSED;
            return NC_ERROR;
        }
        return NC_OK;
    }

    pnlive_server = pool->nlive_server;

    status = server_pool_run(pool);
    if (status != NC_OK) {
        log_error("updating pool %"PRIu32" with dist %d failed: %s", pool->idx,
                  pool->dist_type, strerror(errno));
        return status;
    }

    log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to add %"PRIu32" servers",
              pool->idx, pool->name.len, pool->name.data,
              pool->nlive_server - pnlive_server);


    return NC_OK;
}

static uint32_t
server_pool_hash(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    ASSERT(array_n(&pool->server) != 0);
    ASSERT(key != NULL);

    if (array_n(&pool->server) == 1) {
        return 0;
    }

    if (keylen == 0) {
        return 0;
    }

    return pool->key_hash((char *)key, keylen);
}

uint32_t
server_pool_idx(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    uint32_t hash, idx;

    ASSERT(array_n(&pool->server) != 0);
    ASSERT(key != NULL);

    /*
     * If hash_tag: is configured for this server pool, we use the part of
     * the key within the hash tag as an input to the distributor. Otherwise
     * we use the full key
     */
    if (!string_empty(&pool->hash_tag)) {
        struct string *tag = &pool->hash_tag;
        uint8_t *tag_start, *tag_end;

        tag_start = nc_strchr(key, key + keylen, tag->data[0]);
        if (tag_start != NULL) {
            tag_end = nc_strchr(tag_start + 1, key + keylen, tag->data[1]);
            if ((tag_end != NULL) && (tag_end - tag_start > 1)) {
                key = tag_start + 1;
                keylen = (uint32_t)(tag_end - key);
            }
        }
    }

    switch (pool->dist_type) {
    case DIST_KETAMA:
        hash = server_pool_hash(pool, key, keylen);
        idx = ketama_dispatch(pool->continuum, pool->ncontinuum, hash);
        break;

    case DIST_MODULA:
        hash = server_pool_hash(pool, key, keylen);
        idx = modula_dispatch(pool->continuum, pool->ncontinuum, hash);
        break;

    case DIST_RANDOM:
        idx = random_dispatch(pool->continuum, pool->ncontinuum, 0);
        break;

    case DIST_HASHSLOT:
        hash = server_pool_hash(pool, key, keylen);
        idx = hashslot_dispatch(pool->hashslot, pool->nhashslotnum, hash);
        break;

    default:
        NOT_REACHED();
        return 0;
    }
    ASSERT(idx < array_n(&pool->server));
    return idx;
}

static struct server *
server_pool_server(struct server_pool *pool, uint8_t *key, uint32_t keylen, int pool_write)
{
    struct server *server, *find = NULL;
    uint32_t idx;

    idx = server_pool_idx(pool, key, keylen);

    int server_count = array_n(&pool->server);
    if(server_count <= idx) {
        log_warn("The number of server in the container %"PRIu32" is smaller than the index value:"
                "%"PRIu32"",server_count, idx);
        return NULL;
    }
	
	if (1 == pool_write)
	{
		server = array_get(&pool->server, idx);
	}
	else
	{
		struct server_group* sg = array_get(&pool->server_group, idx);
		uint32_t pool_index = 0;
		if (sg->loop_flag)
		{
			pool_index = sg->current_index % sg->servers.nelem;
			sg->current_index = pool_index + 1;
		}
		else
		{
			pool_index = 0;
		}
		
		server = array_get(&sg->servers, pool_index);
		if (0 == server->connected)
		{
			uint32_t i = 0;
			int64_t now = nc_usec_now();
			if (now >= server->next_retry)
			{
				struct conn* conn = server_conn(server);
				if (conn != NULL)
				{
					server_connect(pool->ctx, server, conn);
				}
			}
			for (; i < sg->servers.nelem; i++)
			{
				find = array_get(&sg->servers, i);
				if (1 == find->connected)
				{
					server = find;
					break;
				}
			}
			
			for (i = 0; i < pool_index; i++)
			{
				find = array_get(&sg->servers, i);
				if (1 == find->connected)
				{
					server = find;
					break;
				}
			}
		}
	}
done:
    log_debug(LOG_VERB, "key '%.*s' on dist %d maps to server '%.*s'", keylen,
              key, pool->dist_type, server->pname.len, server->pname.data);
			  
	if (server)
	{
		printf("%.*s selected, idx:%d,write:%d, pid:%lu\n", server->pname.len, server->pname.data, idx, pool_write, (uint64_t)pthread_self());
	}

    return server;
}

struct conn *
server_pool_conn(struct context *ctx, struct server_pool *pool, uint8_t *key,
                 uint32_t keylen, uint32_t pool_write)
{
    rstatus_t status;
    struct server *server;
    struct conn *conn;

    status = server_pool_update(pool);
    if (status != NC_OK) {
        return NULL;
    }

    /* from a given {key, keylen} pick a server from pool */
//    server = server_pool_server(pool, key, keylen, pool_write);
    server = server_pool_server(pool, key, keylen, 1);
    if (server == NULL) {
        return NULL;
    }
	
    /* pick a connection to a given server */
    conn = server_conn(server);
    if (conn == NULL) {
        return NULL;
    }

    status = server_connect(ctx, server, conn);
    if (status != NC_OK) {
        server_close(ctx, conn);
        return NULL;
    }

    return conn;
}

static rstatus_t
server_pool_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    if (!sp->preconnect) {
        return NC_OK;
    }

    status = array_each(&sp->server, server_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

rstatus_t
server_pool_preconnect(struct context *ctx)
{
    rstatus_t status;

    status = array_each(&ctx->pool, server_pool_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

static rstatus_t
server_pool_each_disconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    status = array_each(&sp->server, server_each_disconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

void
server_pool_disconnect(struct context *ctx)
{
    array_each(&ctx->pool, server_pool_each_disconnect, NULL);
}

static rstatus_t
server_pool_each_set_owner(void *elem, void *data)
{
    struct server_pool *sp = elem;
    struct context *ctx = data;

    sp->ctx = ctx;

    return NC_OK;
}

static rstatus_t
server_pool_each_calc_connections(void *elem, void *data)
{
    struct server_pool *sp = elem;
    struct context *ctx = data;

    ctx->max_nsconn += sp->server_connections * array_n(&sp->server);
    ctx->max_nsconn += 1; /* pool listening socket */

    return NC_OK;
}

rstatus_t
server_pool_run(struct server_pool *pool)
{
    ASSERT(array_n(&pool->server) != 0);

    switch (pool->dist_type) {
    case DIST_KETAMA:
        return ketama_update(pool);

    case DIST_MODULA:
        return modula_update(pool);

    case DIST_RANDOM:
        return random_update(pool);

    case DIST_HASHSLOT:
        return hashslot_update(pool);

    default:
        NOT_REACHED();
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
server_pool_each_run(void *elem, void *data)
{
    return server_pool_run(elem);
}

static rstatus_t
server_pool_each_connected_determine(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    status = array_each(&sp->backup_server, server_each_connected_determine, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

rstatus_t
server_pool_connected_determine(struct context *ctx)
{
    rstatus_t status;
    status = array_each(&ctx->pool, server_pool_each_connected_determine, NULL);
    return NC_OK;
}

rstatus_t
server_pool_init(struct array *server_pool, struct array *conf_pool,
                 struct context *ctx)
{
    rstatus_t status;
    uint32_t npool;

    npool = array_n(conf_pool);
    ASSERT(npool != 0);
    ASSERT(array_n(server_pool) == 0);

    status = array_init(server_pool, npool, sizeof(struct server_pool));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf pool to server pool */
    status = array_each(conf_pool, conf_pool_each_transform, server_pool);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }
    ASSERT(array_n(server_pool) == npool);

    /* set ctx as the server pool owner */
    status = array_each(server_pool, server_pool_each_set_owner, ctx);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    /* compute max server connections */
    ctx->max_nsconn = 0;
    status = array_each(server_pool, server_pool_each_calc_connections, ctx);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    /* update server pool continuum */
    status = array_each(server_pool, server_pool_each_run, NULL);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    log_debug(LOG_DEBUG, "init %"PRIu32" pools", npool);

    return NC_OK;
}

void
server_pool_deinit(struct array *server_pool)
{
    uint32_t i, npool;

    for (i = 0, npool = array_n(server_pool); i < npool; i++) {
        struct server_pool *sp;

        sp = array_pop(server_pool);
        ASSERT(sp->p_conn == NULL);
        ASSERT(TAILQ_EMPTY(&sp->c_conn_q) && sp->nc_conn_q == 0);

        if (sp->continuum != NULL) {
            nc_free(sp->continuum);
            sp->ncontinuum = 0;
            sp->nserver_continuum = 0;
            sp->nlive_server = 0;
        }

        if (sp->hashslot != NULL) {
            nc_free(sp->hashslot);
            sp->nhashslotnum = 0;
            sp->nlive_server = 0;
        }

        server_deinit(&sp->server);
        if(array_n(&sp->backup_server) == array_n(&sp->server)){
            server_deinit(&sp->backup_server);
        }
        if(array_n(&sp->server_identifier) == array_n(&sp->server)){
            server_deinit(&sp->server_identifier);
        }
        if(sp->ssdb_handle){
            dlclose(sp->ssdb_handle);
        }

        log_debug(LOG_DEBUG, "deinit pool %"PRIu32" '%.*s'", sp->idx,
                  sp->name.len, sp->name.data);
    }

    array_deinit(server_pool);

    log_debug(LOG_DEBUG, "deinit %"PRIu32" pools", npool);
}

rstatus_t
server_active_standby_switch(struct server *server)
{
    struct server *backup_server, tmp_server;
    int64_t now;                  /* current timestamp in usec */
    struct server_pool *pool;     /* server pool */
    uint32_t server_index;        /* server index */
    char zk_path[64];
    char back_ip_str[128], server_ip_str[128];
    char zk_set_str[256];

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    pool = server->owner;

    if(server->next_retry > now) {

        server_index = array_idx(&pool->server, server);
        backup_server = array_get(&pool->backup_server, server_index);

        struct String_vector strings;
        int child_ret = zk_get_children(pool->zh_handler, "/nodes", NULL, NULL, &strings);
        if (child_ret) {
            return NC_ERROR;
        } else {
            qsort(strings.data, (size_t)strings.count, sizeof(char *), comp);
        }
//        tmp_server = backup_server;
//        backup_server = server;
//        server = tmp_server;
        memcpy(&tmp_server, backup_server, sizeof(struct server));
        memcpy(backup_server, server, sizeof(struct server));
        memcpy(server, &tmp_server, sizeof(struct server));

        TAILQ_INIT(&server->s_conn_q);
        TAILQ_INIT(&backup_server->s_conn_q);

        if(!pool->master)
        {
            log_warn("not master return");
            return NC_OK;
        }

        if(pool->ssdb_handle) {
            lib_ssdb_active_standby_switch_t active_standby_switch = (lib_ssdb_active_standby_switch_t)dlsym(pool->ssdb_handle, "active_standby_switch");
            int err_no = active_standby_switch((const char*)server->addrstr.data, server->port, &pool->last_seq);
            log_warn("switch between master and slave machines, error_code %d, now master:pool "
                    "%"PRIu32" '%.*s' server address:%.*s:%u, last_seq %lu",err_no, pool->idx,
                    pool->name.len, pool->name.data, server->addrstr.len, server->addrstr.data, server->port, pool->last_seq);

        }else{
            log_warn("ssdb handle is NULL");
            return NC_ERROR;
        }

        struct string back_ip, server_ip;
        uint16_t back_port, server_port;
        back_ip = backup_server->addrstr;
        server_ip = server->addrstr;
        back_port = backup_server->port;
        server_port = server->port;
        memset(back_ip_str, 0, sizeof(back_ip_str));
        memset(server_ip_str, 0, sizeof(server_ip_str));
        strncpy(back_ip_str, back_ip.data, back_ip.len);
        strncpy(server_ip_str, server_ip.data, server_ip.len);
        memset(zk_set_str, 0, sizeof(zk_set_str));
        memset(zk_path, 0, sizeof(zk_path));
        sprintf(zk_set_str, "{\"status\":0,\"ip\":\"%s\",\"port\":%d,\"slave_ip\":\"%s\",\"slave_port\":%d}", server_ip_str, server_port, back_ip_str, back_port);
        sprintf(zk_path, "/nodes/%s", strings.data[server_index]);
        int set_ret = zk_set(pool->zh_handler, zk_path, zk_set_str);
        if (set_ret) {
            return NC_ERROR;
        }
    }
    return NC_OK;
}

