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

#include <nc_core.h>
#include <nc_conf.h>
#include <nc_server.h>
#include <proto/nc_proto.h>
#include <nc_zookeeper.h>

#define DEFINE_ACTION(_hash, _name) string(#_name),
static struct string hash_strings[] = {
    HASH_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_hash, _name) hash_##_name,
static hash_t hash_algos[] = {
    HASH_CODEC( DEFINE_ACTION )
    NULL
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_dist, _name) string(#_name),
static struct string dist_strings[] = {
    DIST_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

static struct command conf_commands[] = {
    { string("listen"),
      conf_set_listen,
      offsetof(struct conf_pool, listen) },

    { string("hash"),
      conf_set_hash,
      offsetof(struct conf_pool, hash) },

    { string("hash_tag"),
      conf_set_hashtag,
      offsetof(struct conf_pool, hash_tag) },

    { string("distribution"),
      conf_set_distribution,
      offsetof(struct conf_pool, distribution) },

    { string("timeout"),
      conf_set_num,
      offsetof(struct conf_pool, timeout) },

    { string("backlog"),
      conf_set_num,
      offsetof(struct conf_pool, backlog) },

    { string("client_connections"),
      conf_set_num,
      offsetof(struct conf_pool, client_connections) },

    { string("protocol"),
      conf_set_protocol,
      offsetof(struct conf_pool, protocol) },

    { string("tcpkeepalive"),
      conf_set_bool,
      offsetof(struct conf_pool, tcpkeepalive) },

    { string("redis_auth"),
      conf_set_string,
      offsetof(struct conf_pool, redis_auth) },

    { string("redis_db"),
      conf_set_num,
      offsetof(struct conf_pool, redis_db) },

    { string("preconnect"),
      conf_set_bool,
      offsetof(struct conf_pool, preconnect) },

    { string("auto_eject_hosts"),
      conf_set_bool,
      offsetof(struct conf_pool, auto_eject_hosts) },

    { string("server_connections"),
      conf_set_num,
      offsetof(struct conf_pool, server_connections) },

    { string("server_retry_timeout"),
      conf_set_num,
      offsetof(struct conf_pool, server_retry_timeout) },

    { string("server_failure_limit"),
      conf_set_num,
      offsetof(struct conf_pool, server_failure_limit) },

    { string("servers"),
      conf_add_server_group,
      offsetof(struct conf_pool, servergroup) },

    { string("backupservers"),
      conf_add_backup_server,
      offsetof(struct conf_pool, backupserver) },

    { string("zookeeperservers"),
      conf_add_zookeeper_server,
      offsetof(struct conf_pool, zookeeperserver) },
  null_command
};

static void
conf_server_init(struct conf_server *cs)
{
    string_init(&cs->pname);
    string_init(&cs->name);
    string_init(&cs->addrstr);
    cs->port = 0;
    cs->weight = 0;

    memset(&cs->info, 0, sizeof(cs->info));

    cs->valid = 0;

    log_debug(LOG_VVERB, "init conf server %p", cs);
}

static void
conf_server_deinit(struct conf_server *cs)
{
    string_deinit(&cs->pname);
    string_deinit(&cs->name);
    string_deinit(&cs->addrstr);
    cs->valid = 0;
    log_debug(LOG_VVERB, "deinit conf server %p", cs);
}

rstatus_t
conf_server_each_transform(void *elem, void *data)
{
    struct conf_server *cs = elem;
    struct array *server = data;
    struct server *s;

    ASSERT(cs->valid);

    s = array_push(server);
    ASSERT(s != NULL);

    s->idx = array_idx(server, s);
    s->owner = NULL;

    s->pname = cs->pname;
    s->name = cs->name;
    s->addrstr = cs->addrstr;
    s->port = (uint16_t)cs->port;
    s->weight = (uint32_t)cs->weight;
	
	s->connected = 1;
	s->is_read = 1;
	s->group = NULL;

    nc_memcpy(&s->info, &cs->info, sizeof(cs->info));

    s->ns_conn_q = 0;
    TAILQ_INIT(&s->s_conn_q);

    s->next_retry = 0LL;
    s->failure_count = 0;

    log_debug(LOG_VERB, "transform to server %"PRIu32" '%.*s'",
              s->idx, s->pname.len, s->pname.data);

    return NC_OK;
}

static rstatus_t
conf_server_init_new(struct conf_server *cs, struct array *server, struct server_pool * sp)
{
    struct server *s;

    ASSERT(cs->valid);

    s = array_push(server);
    ASSERT(s != NULL);

    s->idx = array_idx(server, s);
    s->owner = NULL;

    s->pname = cs->pname;
    s->name = cs->name;
    s->addrstr = cs->addrstr;
    s->port = (uint16_t)cs->port;
    s->weight = (uint32_t)cs->weight;

    s->connected = 1;
    s->is_read = 1;
    s->group = NULL;

    nc_memcpy(&s->info, &cs->info, sizeof(cs->info));

    s->ns_conn_q = 0;
    TAILQ_INIT(&s->s_conn_q);

    s->next_retry = 0LL;
    s->failure_count = 0;

    s->owner = sp;

    log_debug(LOG_VERB, "transform to server %"PRIu32" '%.*s'",
              s->idx, s->pname.len, s->pname.data);

    return NC_OK;
}

static rstatus_t
conf_pool_init(struct conf_pool *cp, struct string *name)
{
    rstatus_t status;

    string_init(&cp->name);

    string_init(&cp->listen.pname);
    string_init(&cp->listen.name);
    string_init(&cp->redis_auth);
    cp->listen.port = 0;
    memset(&cp->listen.info, 0, sizeof(cp->listen.info));
    cp->listen.valid = 0;

    cp->hash = CONF_UNSET_HASH;
    string_init(&cp->hash_tag);
    cp->distribution = CONF_UNSET_DIST;

    cp->timeout = CONF_UNSET_NUM;
    cp->backlog = CONF_UNSET_NUM;

    cp->client_connections = CONF_UNSET_NUM;

    cp->protocol = CONF_UNSET_NUM;
    cp->tcpkeepalive = CONF_UNSET_NUM;
    cp->redis_db = CONF_UNSET_NUM;
    cp->preconnect = CONF_UNSET_NUM;
    cp->auto_eject_hosts = CONF_UNSET_NUM;
    cp->server_connections = CONF_UNSET_NUM;
    cp->server_retry_timeout = CONF_UNSET_NUM;
    cp->server_failure_limit = CONF_UNSET_NUM;

    array_null(&cp->server);

    cp->valid = 0;

    status = string_duplicate(&cp->name, name);
    if (status != NC_OK) {
        return status;
    }

    status = array_init(&cp->server, CONF_DEFAULT_SERVERS,
                        sizeof(struct conf_server));
    if (status != NC_OK) {
        string_deinit(&cp->name);
        return status;
    }
	
	status = array_init(&cp->servergroup, CONF_DEFAULT_SERVERS,
						sizeof(struct conf_server_group));
	if (status != NC_OK) {
        string_deinit(&cp->name);
        return status;
    }

    status = array_init(&cp->backupserver, CONF_DEFAULT_SERVERS,
                        sizeof(struct conf_server));
    if (status != NC_OK) {
        string_deinit(&cp->name);
        return status;
    }

    status = array_init(&cp->zookeeperserver, CONF_DEFAULT_SERVERS,
                        sizeof(struct conf_server));
    if (status != NC_OK) {
        string_deinit(&cp->name);
        return status;
    }
	
    log_debug(LOG_VVERB, "init conf pool %p, '%.*s'", cp, name->len, name->data);

    return NC_OK;
}

static void
conf_pool_deinit(struct conf_pool *cp)
{
    string_deinit(&cp->name);

    string_deinit(&cp->listen.pname);
    string_deinit(&cp->listen.name);

    if (cp->redis_auth.len > 0) {
        string_deinit(&cp->redis_auth);
    }

    while (array_n(&cp->server) != 0) {
        conf_server_deinit(array_pop(&cp->server));
    }
    array_deinit(&cp->server);

    log_debug(LOG_VVERB, "deinit conf pool %p", cp);
}

void
nodes_child_watch(zhandle_t *zh, int type, int state, const char *path,
        void *watcherCtx)
{
    int conf_server_index;
    int server_index;
    uint32_t crc_sb, crc_bs, new_crc;
    uint32_t *crc;
    uint32_t count;
    int conf_index;
    struct array server, backupserver;
    struct conf_server *s, *bs;


    if (type == ZOO_CHILD_EVENT) {
        struct String_vector strings;
        int child_ret = zk_get_children(zh, "/nodes", nodes_child_watch, watcherCtx, &strings);
        if (child_ret) {
            return;
        }

        struct server_pool *sp = (struct server_pool *)watcherCtx;
        rstatus_t status = conf_from_zookeeper(zh, &server, &backupserver, sp);
        if(NC_OK != status){
            return;
        }

        while (1) {
            if(sp->finish_init) {
                break;
            }else{
                sleep(1);
            }
        }

        if(array_n(&server) == 0) {
            return;
        }

        if(array_n(&server) != array_n(&backupserver)) {
            return;
        }

        conf_index = -1;

        for(conf_server_index = (int)array_n(&server) - 1; conf_server_index >= 0; conf_server_index--) {
            s  = array_get(&server, (uint32_t)conf_server_index);
            bs = array_get(&backupserver, (uint32_t)conf_server_index);

            count = 0;

            crc_sb = (hash_crc16((const char *)s->pname.data, s->pname.len) << 16) +
                    hash_crc16((const char *)bs->pname.data, bs->pname.len);
            crc_bs = (hash_crc16((const char *)bs->pname.data, bs->pname.len) << 16) +
                    hash_crc16((const char *)s->pname.data, s->pname.len);

            for(server_index = 0; server_index < (int)array_n(&sp->server_identifier); server_index++) {
                crc     = (uint32_t *)array_get(&sp->server_identifier, (uint32_t)server_index);
                if (crc_sb == *crc || crc_bs == *crc) {
                    break;
                } else {
                    count++;
                }
            }

            if (count == array_n(&sp->server_identifier)) {
                conf_index = server_index;
                new_crc = crc_sb;
                break;
            }
        }

        if(0 < conf_index) {
            conf_server_init_new(array_get(&server, (uint32_t)conf_index), &sp->server, sp);
            conf_server_init_new(array_get(&backupserver, (uint32_t)conf_index), &sp->backup_server, sp);
            crc = array_push(&sp->server_identifier);
            *crc = new_crc;
        }
    }
}

rstatus_t
conf_from_zookeeper(zhandle_t *zh_handler, struct array * servers, struct array *backup_servers, struct server_pool *sp)
{
    struct String_vector strings;
    int child_ret = zk_get_children(zh_handler, "/nodes", nodes_child_watch, sp, &strings);
    if (child_ret) {
        return NC_ERROR;
    } else {
        int child_num = strings.count;
        if (child_num == 0) {
            return NC_ERROR;
        } else {
            qsort(strings.data, (size_t)strings.count, sizeof(char *), comp);
            rstatus_t init_status;
            init_status = array_init(servers, CONF_DEFAULT_SERVERS, sizeof(struct conf_server));
            if (init_status != NC_OK) {
                return init_status;
            }
            init_status = array_init(backup_servers, CONF_DEFAULT_SERVERS, sizeof(struct conf_server));
            if (init_status != NC_OK) {
                return init_status;
            }

            int zk_index;
            char data[CONF_DEFAULT_DATA_LENGTH];
            char zk_path[50];
            size_t pathlen = sizeof(zk_path);
            for (zk_index = 0; zk_index < child_num; ++zk_index) {
                int datalen = CONF_DEFAULT_DATA_LENGTH;
                memset(data, 0, datalen);
                memset(zk_path, 0, pathlen);
                sprintf(zk_path, "/nodes/%s", strings.data[zk_index]);
                int get_ret = zk_get(zh_handler, ((const char *)zk_path), NULL, NULL, data, &datalen);
                if (get_ret) {
                    return NC_ERROR;
                }
                json_object *json_data = json_tokener_parse(data);
                const char *server_str = NULL;
                const char *back_str = NULL;
                char server_temp[64], back_temp[64];
                int server_port, back_port;
                JSON_GET_STR(server_str, json_data, "ip", server_temp);
                JSON_GET_STR(back_str, json_data, "slave_ip", back_temp);
                JSON_GET_INT32(json_data, "port", &server_port, 0);
                JSON_GET_INT32(json_data, "slave_port", &back_port, 0);
                json_object_put(json_data);
                if (!server_str || !back_str)
                {
                    log_error("no server or backupserver conf");
                    return NC_ERROR;
                }

                struct conf_server *server_field, *back_field;
                server_field = array_push(servers);
                back_field = array_push(backup_servers);
                conf_server_init(server_field);
                conf_server_init(back_field);

                struct string server_pname, back_pname, server_name, back_name;
                string_init(&server_pname);
                string_init(&back_pname);
                string_init(&server_name);
                string_init(&back_name);
                char server_pstr[64], back_pstr[64];
                memset(server_pstr, 0, sizeof(server_pstr));
                memset(back_pstr, 0, sizeof(back_pstr));
                sprintf(server_pstr, "%s:%d", server_str, server_port);
                sprintf(back_pstr, "%s:%d", back_str, back_port);
                string_copy(&server_pname, (const uint8_t *)server_pstr, (uint32_t)strlen(server_pstr));
                string_copy(&back_pname, (const uint8_t *)back_pstr, (uint32_t)strlen(back_pstr));
                string_copy(&server_name, (const uint8_t *)server_str, (uint32_t)strlen(server_str));
                string_copy(&back_name, (const uint8_t *)back_str, (uint32_t)strlen(back_str));

                rstatus_t status;
                status = string_duplicate(&server_field->pname, &server_pname);
                if (status != NC_OK) {
                    return status;
                }
                status = string_duplicate(&server_field->name, &server_name);
                if (status != NC_OK) {
                    return status;
                }
                status = string_duplicate(&server_field->addrstr, &server_name);
                if (status != NC_OK) {
                    return status;
                }
                server_field->port = server_port;
                server_field->weight = 1;

                status = string_duplicate(&back_field->pname, &back_pname);
                if (status != NC_OK) {
                    return status;
                }
                status = string_duplicate(&back_field->name, &back_name);
                if (status != NC_OK) {
                    return status;
                }
                status = string_duplicate(&back_field->addrstr, &back_name);
                if (status != NC_OK) {
                    return status;
                }
                back_field->port = back_port;
                back_field->weight = 1;
            }
        }
    }

    return NC_OK;
}

void init_watcher(zhandle_t *zh, int type, int state, const char *path,
        void* context)
{
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
        }
        else if (state == ZOO_EXPIRED_SESSION_STATE) {
            ((struct zk_init_ctx *) context)->zh = zk_init(((struct zk_init_ctx *) context)->host, init_watcher, ((struct zk_init_ctx *) context)->timeout, (struct zk_init_ctx *)context);
        }
    }
}

rstatus_t
conf_pool_each_transform(void *elem, void *data)
{
    rstatus_t status;
    struct conf_pool *cp = elem;
    struct array *server_pool = data;
    struct server_pool *sp;
    int zk_index;

    ASSERT(cp->valid);

    sp = array_push(server_pool);
    ASSERT(sp != NULL);

    sp->idx = array_idx(server_pool, sp);
    sp->ctx = NULL;
    sp->finish_init = 0;

    sp->p_conn = NULL;
    sp->nc_conn_q = 0;
    TAILQ_INIT(&sp->c_conn_q);

    array_null(&sp->server);
    sp->ncontinuum = 0;
    sp->nserver_continuum = 0;
    sp->continuum = NULL;
    sp->hashslot  = NULL;
    sp->nlive_server = 0;
    sp->next_rebuild = 0LL;

    sp->name = cp->name;
    sp->addrstr = cp->listen.pname;
    sp->port = (uint16_t)cp->listen.port;

    nc_memcpy(&sp->info, &cp->listen.info, sizeof(cp->listen.info));
    sp->perm = cp->listen.perm;

    sp->key_hash_type = cp->hash;
    sp->key_hash = hash_algos[cp->hash];
    sp->dist_type = cp->distribution;
    sp->hash_tag = cp->hash_tag;

    sp->tcpkeepalive = cp->tcpkeepalive ? 1 : 0;

    sp->protocol = (uint32_t)cp->protocol;
    sp->timeout = cp->timeout;
    sp->backlog = cp->backlog;
    sp->redis_db = cp->redis_db;

    sp->redis_auth = cp->redis_auth;
    sp->require_auth = cp->redis_auth.len > 0 ? 1 : 0;

    sp->client_connections = (uint32_t)cp->client_connections;
    sp->server_connections = (uint32_t)cp->server_connections;
    sp->server_retry_timeout = (int64_t)cp->server_retry_timeout * 1000LL;
    sp->server_failure_limit = (uint32_t)cp->server_failure_limit;
    sp->auto_eject_hosts = cp->auto_eject_hosts ? 1 : 0;
    sp->preconnect = cp->preconnect ? 1 : 0;

    if (array_n(&cp->zookeeperserver) != 0) {
        char zk_host[256];
        size_t zk_len = sizeof(zk_host);
        memset(zk_host, 0, zk_len);
        for (zk_index = 0; zk_index < array_n(&cp->zookeeperserver); ++zk_index) {
            char zk_one_host[128];
            size_t zk_one_len = sizeof(zk_one_host);
            memset(zk_one_host, 0, zk_one_len);
            sprintf(zk_one_host, "%s:%d", ((struct conf_server *)array_get(&cp->zookeeperserver, zk_index))->addrstr.data, ((struct conf_server *)array_get(&cp->zookeeperserver, zk_index))->port);
            if (zk_index == 0) {
                strcpy(zk_host, zk_one_host);
            }
            else {
                sprintf(zk_host, "%s,%s", zk_host, zk_one_host);
            }
        }
        int init_timeout = 30000;
        sp->init_ctx = nc_alloc(sizeof(struct zk_init_ctx));
        sp->init_ctx->host = zk_host;
        sp->init_ctx->timeout = init_timeout;
        sp->init_ctx->zh = sp->zh_handler;
        sp->zh_handler = zk_init(zk_host, init_watcher, init_timeout, sp->init_ctx);
        status = conf_from_zookeeper(sp->zh_handler, &cp->server, &cp->backupserver, sp);
        if (status != NC_OK) {
            return status;
        }
    }

    if (array_n(&cp->backupserver) != 0) {
        if (array_n(&cp->backupserver) != array_n(&cp->server)) {
            log_error("The number of servers is not the same as the number of backserver");
            return NC_ERROR;
        }

        status = server_init(&sp->server, &cp->server, sp);
        if (status != NC_OK) {
            log_error("server init error");
            return status;
        }

        status = backup_server_init(&sp->backup_server, &cp->backupserver, sp);
        if (status != NC_OK) {
            log_error("backupserver init error");
            return status;
        }

        status = server_identifier_init(&sp->server, &sp->backup_server, sp);
        if (status != NC_OK) {
            log_error("server_identifier init error");
            return status;
        }

        status = slotmap_ctx_init(&sp->ctx_array);
        if (status != NC_OK) {
            log_error("slot map context init error");
            return status;
        }
    }

    sp->finish_init = 1;

    log_debug(LOG_VERB, "transform to pool %"PRIu32" '%.*s'", sp->idx,
              sp->name.len, sp->name.data);

    return NC_OK;
}

static void
conf_dump(struct conf *cf)
{
    uint32_t i, j, npool, nserver;
    struct conf_pool *cp;
    struct string *s;

    npool = array_n(&cf->pool);
    if (npool == 0) {
        return;
    }

    log_debug(LOG_VVERB, "%"PRIu32" pools in configuration file '%s'", npool,
              cf->fname);

    for (i = 0; i < npool; i++) {
        cp = array_get(&cf->pool, i);

        log_debug(LOG_VVERB, "%.*s", cp->name.len, cp->name.data);
        log_debug(LOG_VVERB, "  listen: %.*s",
                  cp->listen.pname.len, cp->listen.pname.data);
        log_debug(LOG_VVERB, "  timeout: %d", cp->timeout);
        log_debug(LOG_VVERB, "  backlog: %d", cp->backlog);
        log_debug(LOG_VVERB, "  hash: %d", cp->hash);
        log_debug(LOG_VVERB, "  hash_tag: \"%.*s\"", cp->hash_tag.len,
                  cp->hash_tag.data);
        log_debug(LOG_VVERB, "  distribution: %d", cp->distribution);
        log_debug(LOG_VVERB, "  client_connections: %d",
                  cp->client_connections);
        log_debug(LOG_VVERB, "  protocol: %d", cp->protocol);
        log_debug(LOG_VVERB, "  preconnect: %d", cp->preconnect);
        log_debug(LOG_VVERB, "  auto_eject_hosts: %d", cp->auto_eject_hosts);
        log_debug(LOG_VVERB, "  server_connections: %d",
                  cp->server_connections);
        log_debug(LOG_VVERB, "  server_retry_timeout: %d",
                  cp->server_retry_timeout);
        log_debug(LOG_VVERB, "  server_failure_limit: %d",
                  cp->server_failure_limit);

        nserver = array_n(&cp->server);
        log_debug(LOG_VVERB, "  servers: %"PRIu32"", nserver);

        for (j = 0; j < nserver; j++) {
            s = array_get(&cp->server, j);
            log_debug(LOG_VVERB, "    %.*s", s->len, s->data);
        }
    }
}

static rstatus_t
conf_yaml_init(struct conf *cf)
{
    int rv;

    ASSERT(!cf->valid_parser);

    rv = fseek(cf->fh, 0L, SEEK_SET);
    if (rv < 0) {
        log_error("conf: failed to seek to the beginning of file '%s': %s",
                  cf->fname, strerror(errno));
        return NC_ERROR;
    }

    rv = yaml_parser_initialize(&cf->parser);
    if (!rv) {
        log_error("conf: failed (err %d) to initialize yaml parser",
                  cf->parser.error);
        return NC_ERROR;
    }

    yaml_parser_set_input_file(&cf->parser, cf->fh);
    cf->valid_parser = 1;

    return NC_OK;
}

static void
conf_yaml_deinit(struct conf *cf)
{
    if (cf->valid_parser) {
        yaml_parser_delete(&cf->parser);
        cf->valid_parser = 0;
    }
}

static rstatus_t
conf_token_next(struct conf *cf)
{
    int rv;

    ASSERT(cf->valid_parser && !cf->valid_token);

    rv = yaml_parser_scan(&cf->parser, &cf->token);
    if (!rv) {
        log_error("conf: failed (err %d) to scan next token", cf->parser.error);
        return NC_ERROR;
    }
    cf->valid_token = 1;

    return NC_OK;
}

static void
conf_token_done(struct conf *cf)
{
    ASSERT(cf->valid_parser);

    if (cf->valid_token) {
        yaml_token_delete(&cf->token);
        cf->valid_token = 0;
    }
}

static rstatus_t
conf_event_next(struct conf *cf)
{
    int rv;

    ASSERT(cf->valid_parser && !cf->valid_event);

    rv = yaml_parser_parse(&cf->parser, &cf->event);
    if (!rv) {
        log_error("conf: failed (err %d) to get next event", cf->parser.error);
        return NC_ERROR;
    }
    cf->valid_event = 1;

    return NC_OK;
}

static void
conf_event_done(struct conf *cf)
{
    if (cf->valid_event) {
        yaml_event_delete(&cf->event);
        cf->valid_event = 0;
    }
}

static rstatus_t
conf_push_scalar(struct conf *cf)
{
    rstatus_t status;
    struct string *value;
    uint8_t *scalar;
    uint32_t scalar_len;

    scalar = cf->event.data.scalar.value;
    scalar_len = (uint32_t)cf->event.data.scalar.length;
    if (scalar_len == 0) {
        return NC_ERROR;
    }

    log_debug(LOG_VVERB, "push '%.*s'", scalar_len, scalar);

    value = array_push(&cf->arg);
    if (value == NULL) {
        return NC_ENOMEM;
    }
    string_init(value);

    status = string_copy(value, scalar, scalar_len);
    if (status != NC_OK) {
        array_pop(&cf->arg);
        return status;
    }

    return NC_OK;
}

static void
conf_pop_scalar(struct conf *cf)
{
    struct string *value;

    value = array_pop(&cf->arg);
    log_debug(LOG_VVERB, "pop '%.*s'", value->len, value->data);
    string_deinit(value);
}

static rstatus_t
conf_handler(struct conf *cf, void *data)
{
    struct command *cmd;
    struct string *key, *value;
    uint32_t narg;

    if (array_n(&cf->arg) == 1) {
        value = array_top(&cf->arg);
        log_debug(LOG_VVERB, "conf handler on '%.*s'", value->len, value->data);
        return conf_pool_init(data, value);
    }

    narg = array_n(&cf->arg);
    value = array_get(&cf->arg, narg - 1);
    key = array_get(&cf->arg, narg - 2);

    log_debug(LOG_VVERB, "conf handler on %.*s: %.*s", key->len, key->data,
              value->len, value->data);

    for (cmd = conf_commands; cmd->name.len != 0; cmd++) {
        char *rv;

        if (string_compare(key, &cmd->name) != 0) {
            continue;
        }

        rv = cmd->set(cf, cmd, data);
        if (rv != CONF_OK) {
            log_error("conf: directive \"%.*s\" %s", key->len, key->data, rv);
            return NC_ERROR;
        }

        return NC_OK;
    }

    log_error("conf: directive \"%.*s\" is unknown", key->len, key->data);

    return NC_ERROR;
}

static rstatus_t
conf_begin_parse(struct conf *cf)
{
    rstatus_t status;
    bool done;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(cf->depth == 0);

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        log_debug(LOG_VVERB, "next begin event %d", cf->event.type);

        switch (cf->event.type) {
        case YAML_STREAM_START_EVENT:
        case YAML_DOCUMENT_START_EVENT:
            break;

        case YAML_MAPPING_START_EVENT:
            ASSERT(cf->depth < CONF_MAX_DEPTH);
            cf->depth++;
            done = true;
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);

    } while (!done);

    return NC_OK;
}

static rstatus_t
conf_end_parse(struct conf *cf)
{
    rstatus_t status;
    bool done;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(cf->depth == 0);

    done = false;
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        log_debug(LOG_VVERB, "next end event %d", cf->event.type);

        switch (cf->event.type) {
        case YAML_STREAM_END_EVENT:
            done = true;
            break;

        case YAML_DOCUMENT_END_EVENT:
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);
    } while (!done);

    conf_yaml_deinit(cf);

    return NC_OK;
}

static rstatus_t
conf_parse_core(struct conf *cf, void *data)
{
    rstatus_t status;
    bool done, leaf, new_pool;

    ASSERT(cf->sound);

    status = conf_event_next(cf);
    if (status != NC_OK) {
        return status;
    }

    log_debug(LOG_VVERB, "next event %d depth %"PRIu32" seq %d", cf->event.type,
              cf->depth, cf->seq);

    done = false;
    leaf = false;
    new_pool = false;

    switch (cf->event.type) {
    case YAML_MAPPING_END_EVENT:
        cf->depth--;
        if (cf->depth == 1) {
            conf_pop_scalar(cf);
        } else if (cf->depth == 0) {
            done = true;
        }
        break;

    case YAML_MAPPING_START_EVENT:
        cf->depth++;
        break;

    case YAML_SEQUENCE_START_EVENT:
        cf->seq = 1;
        break;

    case YAML_SEQUENCE_END_EVENT:
        conf_pop_scalar(cf);
        cf->seq = 0;
        break;

    case YAML_SCALAR_EVENT:
        status = conf_push_scalar(cf);
        if (status != NC_OK) {
            break;
        }

        /* take appropriate action */
        if (cf->seq) {
            /* for a sequence, leaf is at CONF_MAX_DEPTH */
            ASSERT(cf->depth == CONF_MAX_DEPTH);
            leaf = true;
        } else if (cf->depth == CONF_ROOT_DEPTH) {
            /* create new conf_pool */
            data = array_push(&cf->pool);
            if (data == NULL) {
                status = NC_ENOMEM;
                break;
           }
           new_pool = true;
        } else if (array_n(&cf->arg) == cf->depth + 1) {
            /* for {key: value}, leaf is at CONF_MAX_DEPTH */
            ASSERT(cf->depth == CONF_MAX_DEPTH);
            leaf = true;
        }
        break;

    default:
        NOT_REACHED();
        break;
    }

    conf_event_done(cf);

    if (status != NC_OK) {
        return status;
    }

    if (done) {
        /* terminating condition */
        return NC_OK;
    }

    if (leaf || new_pool) {
        status = conf_handler(cf, data);

        if (leaf) {
            conf_pop_scalar(cf);
            if (!cf->seq) {
                conf_pop_scalar(cf);
            }
        }

        if (status != NC_OK) {
            return status;
        }
    }

    return conf_parse_core(cf, data);
}

static rstatus_t
conf_parse(struct conf *cf)
{
    rstatus_t status;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(array_n(&cf->arg) == 0);

    status = conf_begin_parse(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_parse_core(cf, NULL);
    if (status != NC_OK) {
        return status;
    }

    status = conf_end_parse(cf);
    if (status != NC_OK) {
        return status;
    }

    cf->parsed = 1;

    return NC_OK;
}

static struct conf *
conf_open(char *filename)
{
    rstatus_t status;
    struct conf *cf;
    FILE *fh;

    fh = fopen(filename, "r");
    if (fh == NULL) {
        log_error("conf: failed to open configuration '%s': %s", filename,
                  strerror(errno));
        return NULL;
    }

    cf = nc_alloc(sizeof(*cf));
    if (cf == NULL) {
        fclose(fh);
        return NULL;
    }

    status = array_init(&cf->arg, CONF_DEFAULT_ARGS, sizeof(struct string));
    if (status != NC_OK) {
        nc_free(cf);
        fclose(fh);
        return NULL;
    }

    status = array_init(&cf->pool, CONF_DEFAULT_POOL, sizeof(struct conf_pool));
    if (status != NC_OK) {
        array_deinit(&cf->arg);
        nc_free(cf);
        fclose(fh);
        return NULL;
    }

    cf->fname = filename;
    cf->fh = fh;
    cf->depth = 0;
    /* parser, event, and token are initialized later */
    cf->seq = 0;
    cf->valid_parser = 0;
    cf->valid_event = 0;
    cf->valid_token = 0;
    cf->sound = 0;
    cf->parsed = 0;
    cf->valid = 0;

    log_debug(LOG_VVERB, "opened conf '%s'", filename);

    return cf;
}

static rstatus_t
conf_validate_document(struct conf *cf)
{
    rstatus_t status;
    uint32_t count;
    bool done;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    count = 0;
    done = false;
    do {
        yaml_document_t document;
        yaml_node_t *node;
        int rv;

        rv = yaml_parser_load(&cf->parser, &document);
        if (!rv) {
            log_error("conf: failed (err %d) to get the next yaml document",
                      cf->parser.error);
            conf_yaml_deinit(cf);
            return NC_ERROR;
        }

        node = yaml_document_get_root_node(&document);
        if (node == NULL) {
            done = true;
        } else {
            count++;
        }

        yaml_document_delete(&document);
    } while (!done);

    conf_yaml_deinit(cf);

    if (count != 1) {
        log_error("conf: '%s' must contain only 1 document; found %"PRIu32" "
                  "documents", cf->fname, count);
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
conf_validate_tokens(struct conf *cf)
{
    rstatus_t status;
    bool done, error;
    int type;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    error = false;
    do {
        status = conf_token_next(cf);
        if (status != NC_OK) {
            return status;
        }
        type = cf->token.type;

        switch (type) {
        case YAML_NO_TOKEN:
            error = true;
            log_error("conf: no token (%d) is disallowed", type);
            break;

        case YAML_VERSION_DIRECTIVE_TOKEN:
            error = true;
            log_error("conf: version directive token (%d) is disallowed", type);
            break;

        case YAML_TAG_DIRECTIVE_TOKEN:
            error = true;
            log_error("conf: tag directive token (%d) is disallowed", type);
            break;

        case YAML_DOCUMENT_START_TOKEN:
            error = true;
            log_error("conf: document start token (%d) is disallowed", type);
            break;

        case YAML_DOCUMENT_END_TOKEN:
            error = true;
            log_error("conf: document end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_SEQUENCE_START_TOKEN:
            error = true;
            log_error("conf: flow sequence start token (%d) is disallowed", type);
            break;

        case YAML_FLOW_SEQUENCE_END_TOKEN:
            error = true;
            log_error("conf: flow sequence end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_MAPPING_START_TOKEN:
            error = true;
            log_error("conf: flow mapping start token (%d) is disallowed", type);
            break;

        case YAML_FLOW_MAPPING_END_TOKEN:
            error = true;
            log_error("conf: flow mapping end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_ENTRY_TOKEN:
            error = true;
            log_error("conf: flow entry token (%d) is disallowed", type);
            break;

        case YAML_ALIAS_TOKEN:
            error = true;
            log_error("conf: alias token (%d) is disallowed", type);
            break;

        case YAML_ANCHOR_TOKEN:
            error = true;
            log_error("conf: anchor token (%d) is disallowed", type);
            break;

        case YAML_TAG_TOKEN:
            error = true;
            log_error("conf: tag token (%d) is disallowed", type);
            break;

        case YAML_BLOCK_SEQUENCE_START_TOKEN:
        case YAML_BLOCK_MAPPING_START_TOKEN:
        case YAML_BLOCK_END_TOKEN:
        case YAML_BLOCK_ENTRY_TOKEN:
            break;

        case YAML_KEY_TOKEN:
        case YAML_VALUE_TOKEN:
        case YAML_SCALAR_TOKEN:
            break;

        case YAML_STREAM_START_TOKEN:
            break;

        case YAML_STREAM_END_TOKEN:
            done = true;
            log_debug(LOG_VVERB, "conf '%s' has valid tokens", cf->fname);
            break;

        default:
            error = true;
            log_error("conf: unknown token (%d) is disallowed", type);
            break;
        }

        conf_token_done(cf);
    } while (!done && !error);

    conf_yaml_deinit(cf);

    return !error ? NC_OK : NC_ERROR;
}

static rstatus_t
conf_validate_structure(struct conf *cf)
{
    rstatus_t status;
    int type, depth;
    uint32_t i, count[CONF_MAX_DEPTH + 1];
    bool done, error, seq;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    error = false;
    seq = false;
    depth = 0;
    for (i = 0; i < CONF_MAX_DEPTH + 1; i++) {
        count[i] = 0;
    }

    /*
     * Validate that the configuration conforms roughly to the following
     * yaml tree structure:
     *
     * keyx:
     *   key1: value1
     *   key2: value2
     *   seq:
     *     - elem1
     *     - elem2
     *     - elem3
     *   key3: value3
     *
     * keyy:
     *   key1: value1
     *   key2: value2
     *   seq:
     *     - elem1
     *     - elem2
     *     - elem3
     *   key3: value3
     */
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        type = cf->event.type;

        log_debug(LOG_VVERB, "next event %d depth %d seq %d", type, depth, seq);

        switch (type) {
        case YAML_STREAM_START_EVENT:
        case YAML_DOCUMENT_START_EVENT:
            break;

        case YAML_DOCUMENT_END_EVENT:
            break;

        case YAML_STREAM_END_EVENT:
            done = true;
            break;

        case YAML_MAPPING_START_EVENT:
            if (depth == CONF_ROOT_DEPTH && count[depth] != 1) {
                error = true;
                log_error("conf: '%s' has more than one \"key:value\" at depth"
                          " %d", cf->fname, depth);
            } else if (depth >= CONF_MAX_DEPTH) {
                error = true;
                log_error("conf: '%s' has a depth greater than %d", cf->fname,
                          CONF_MAX_DEPTH);
            }
            depth++;
            break;

        case YAML_MAPPING_END_EVENT:
            if (depth == CONF_MAX_DEPTH) {
                if (seq) {
                    seq = false;
                } else {
                    error = true;
                    log_error("conf: '%s' missing sequence directive at depth "
                              "%d", cf->fname, depth);
                }
            }
            depth--;
            count[depth] = 0;
            break;

        case YAML_SEQUENCE_START_EVENT:
            if (seq) {
                //error = true;
                //log_error("conf: '%s' has more than one sequence directive",
                         // cf->fname);
            } else if (depth != CONF_MAX_DEPTH) {
                error = true;
                log_error("conf: '%s' has sequence at depth %d instead of %d",
                          cf->fname, depth, CONF_MAX_DEPTH);
            } else if (count[depth] != 1) {
                error = true;
                log_error("conf: '%s' has invalid \"key:value\" at depth %d",
                          cf->fname, depth);
            }
            seq = true;
            break;

        case YAML_SEQUENCE_END_EVENT:
            ASSERT(depth == CONF_MAX_DEPTH);
            count[depth] = 0;
            break;

        case YAML_SCALAR_EVENT:
            if (depth == 0) {
                error = true;
                log_error("conf: '%s' has invalid empty \"key:\" at depth %d",
                          cf->fname, depth);
            } else if (depth == CONF_ROOT_DEPTH && count[depth] != 0) {
                error = true;
                log_error("conf: '%s' has invalid mapping \"key:\" at depth %d",
                          cf->fname, depth);
            } else if (depth == CONF_MAX_DEPTH && count[depth] == 2) {
                /* found a "key: value", resetting! */
                count[depth] = 0;
            }
            count[depth]++;
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);
    } while (!done && !error);

    conf_yaml_deinit(cf);

    return !error ? NC_OK : NC_ERROR;
}

static rstatus_t
conf_pre_validate(struct conf *cf)
{
    rstatus_t status;

    status = conf_validate_document(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_validate_tokens(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_validate_structure(cf);
    if (status != NC_OK) {
        return status;
    }

    cf->sound = 1;

    return NC_OK;
}

static int
conf_pool_name_cmp(const void *t1, const void *t2)
{
    const struct conf_pool *p1 = t1, *p2 = t2;

    return string_compare(&p1->name, &p2->name);
}

static int
conf_pool_listen_cmp(const void *t1, const void *t2)
{
    const struct conf_pool *p1 = t1, *p2 = t2;

    return string_compare(&p1->listen.pname, &p2->listen.pname);
}

static rstatus_t
conf_validate_server(struct conf *cf, struct conf_pool *cp)
{
    uint32_t nserver;

    nserver = array_n(&cp->server);
    if (nserver == 0) {
        log_error("conf: pool '%.*s' has no servers", cp->name.len,
                  cp->name.data);
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
conf_validate_pool(struct conf *cf, struct conf_pool *cp)
{
    rstatus_t status;

    ASSERT(!cp->valid);
    ASSERT(!string_empty(&cp->name));

    if (!cp->listen.valid) {
        log_error("conf: directive \"listen:\" is missing");
        return NC_ERROR;
    }

    /* set default values for unset directives */

    if (cp->distribution == CONF_UNSET_DIST) {
        cp->distribution = CONF_DEFAULT_DIST;
    }

    if (cp->hash == CONF_UNSET_HASH) {
        cp->hash = CONF_DEFAULT_HASH;
    }

    if (cp->timeout == CONF_UNSET_NUM) {
        cp->timeout = CONF_DEFAULT_TIMEOUT;
    }

    if (cp->backlog == CONF_UNSET_NUM) {
        cp->backlog = CONF_DEFAULT_LISTEN_BACKLOG;
    }

    cp->client_connections = CONF_DEFAULT_CLIENT_CONNECTIONS;

    if (cp->protocol == CONF_UNSET_NUM) {
        cp->protocol = CONF_DEFAULT_REDIS;
    }

    if (cp->tcpkeepalive == CONF_UNSET_NUM) {
        cp->tcpkeepalive = CONF_DEFAULT_TCPKEEPALIVE;
    }

    if (cp->redis_db == CONF_UNSET_NUM) {
        cp->redis_db = CONF_DEFAULT_REDIS_DB;
    }

    if (cp->preconnect == CONF_UNSET_NUM) {
        cp->preconnect = CONF_DEFAULT_PRECONNECT;
    }

    if (cp->auto_eject_hosts == CONF_UNSET_NUM) {
        cp->auto_eject_hosts = CONF_DEFAULT_AUTO_EJECT_HOSTS;
    }

    if (cp->server_connections == CONF_UNSET_NUM) {
        cp->server_connections = CONF_DEFAULT_SERVER_CONNECTIONS;
    } else if (cp->server_connections == 0) {
        log_error("conf: directive \"server_connections:\" cannot be 0");
        return NC_ERROR;
    }

    if (cp->server_retry_timeout == CONF_UNSET_NUM) {
        cp->server_retry_timeout = CONF_DEFAULT_SERVER_RETRY_TIMEOUT;
    }

    if (cp->server_failure_limit == CONF_UNSET_NUM) {
        cp->server_failure_limit = CONF_DEFAULT_SERVER_FAILURE_LIMIT;
    }

    if (cp->protocol != PROTOCOL_REDIS && cp->redis_auth.len > 0) {
        log_error("conf: directive \"redis_auth:\" is only valid for a redis pool");
        return NC_ERROR;
    }
	
	if (0 == array_n(&cp->server) && array_n(&cp->servergroup) > 0)
	{
		uint32_t size = array_n(&cp->servergroup);
		uint32_t i;
		struct conf_server_group* group = cp->servergroup.elem;
		for (i = 0; i < size; i++)
		{
			struct conf_server* server = group->server.elem;
			struct conf_server* copy   = array_push(&cp->server);
			memcpy(copy, server, sizeof(struct conf_server));
			group++;
		}
	}
    status = conf_validate_server(cf, cp);
    if (status != NC_OK) {
        return status;
    }

    cp->valid = 1;

    return NC_OK;
}

static rstatus_t
conf_post_validate(struct conf *cf)
{
    rstatus_t status;
    uint32_t i, npool;
    bool valid;

    ASSERT(cf->sound && cf->parsed);
    ASSERT(!cf->valid);

    npool = array_n(&cf->pool);
    if (npool == 0) {
        log_error("conf: '%.*s' has no pools", cf->fname);
        return NC_ERROR;
    }

    /* validate pool */
    for (i = 0; i < npool; i++) {
        struct conf_pool *cp = array_get(&cf->pool, i);

        status = conf_validate_pool(cf, cp);
        if (status != NC_OK) {
            return status;
        }
    }

    /* disallow pools with duplicate listen: key values */
    array_sort(&cf->pool, conf_pool_listen_cmp);
    for (valid = true, i = 0; i < npool - 1; i++) {
        struct conf_pool *p1, *p2;

        p1 = array_get(&cf->pool, i);
        p2 = array_get(&cf->pool, i + 1);

        if (string_compare(&p1->listen.pname, &p2->listen.pname) == 0) {
            log_error("conf: pools '%.*s' and '%.*s' have the same listen "
                      "address '%.*s'", p1->name.len, p1->name.data,
                      p2->name.len, p2->name.data, p1->listen.pname.len,
                      p1->listen.pname.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    /* disallow pools with duplicate names */
    array_sort(&cf->pool, conf_pool_name_cmp);
    for (valid = true, i = 0; i < npool - 1; i++) {
        struct conf_pool *p1, *p2;

        p1 = array_get(&cf->pool, i);
        p2 = array_get(&cf->pool, i + 1);

        if (string_compare(&p1->name, &p2->name) == 0) {
            log_error("conf: '%s' has pools with same name %.*s'", cf->fname,
                      p1->name.len, p1->name.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    return NC_OK;
}

struct conf *
conf_create(char *filename)
{
    rstatus_t status;
    struct conf *cf;

    cf = conf_open(filename);
    if (cf == NULL) {
        return NULL;
    }

    /* validate configuration file before parsing */
    status = conf_pre_validate(cf);
    if (status != NC_OK) {
        goto error;
    }

    /* parse the configuration file */
    status = conf_parse(cf);
    if (status != NC_OK) {
        goto error;
    }

    /* validate parsed configuration */
    status = conf_post_validate(cf);
    if (status != NC_OK) {
        goto error;
    }

    conf_dump(cf);

    fclose(cf->fh);
    cf->fh = NULL;

    return cf;

error:
    log_stderr("nutcracker: configuration file '%s' syntax is invalid",
               filename);
    fclose(cf->fh);
    cf->fh = NULL;
    conf_destroy(cf);
    return NULL;
}

void
conf_destroy(struct conf *cf)
{
    while (array_n(&cf->arg) != 0) {
        conf_pop_scalar(cf);
    }
    array_deinit(&cf->arg);

    while (array_n(&cf->pool) != 0) {
        conf_pool_deinit(array_pop(&cf->pool));
    }
    array_deinit(&cf->pool);

    nc_free(cf);
}

char *
conf_set_string(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    status = string_duplicate(field, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}


char *
conf_set_listen(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    struct string *value;
    struct conf_listen *field;
    uint8_t *p, *name;
    uint32_t namelen;

    p = conf;
    field = (struct conf_listen *)(p + cmd->offset);

    if (field->valid == 1) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    status = string_duplicate(&field->pname, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    if (value->data[0] == '/') {
        uint8_t *q, *start, *perm;
        uint32_t permlen;


        /* parse "socket_path permissions" from the end */
        p = value->data + value->len -1;
        start = value->data;
        q = nc_strrchr(p, start, ' ');
        if (q == NULL) {
            /* no permissions field, so use defaults */
            name = value->data;
            namelen = value->len;
        } else {
            perm = q + 1;
            permlen = (uint32_t)(p - perm + 1);

            p = q - 1;
            name = start;
            namelen = (uint32_t)(p - start + 1);

            errno = 0;
            field->perm = (mode_t)strtol((char *)perm, NULL, 8);
            if (errno || field->perm > 0777) {
                return "has an invalid file permission in \"socket_path permission\" format string";
            }
        }
    } else {
        uint8_t *q, *start, *port;
        uint32_t portlen;

        /* parse "hostname:port" from the end */
        p = value->data + value->len - 1;
        start = value->data;
        q = nc_strrchr(p, start, ':');
        if (q == NULL) {
            return "has an invalid \"hostname:port\" format string";
        }

        port = q + 1;
        portlen = (uint32_t)(p - port + 1);

        p = q - 1;

        name = start;
        namelen = (uint32_t)(p - start + 1);

        field->port = nc_atoi(port, portlen);
        if (field->port < 0 || !nc_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port\" format string";
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = nc_resolve(&field->name, field->port, &field->info);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    field->valid = 1;

    return CONF_OK;
}

char * conf_add_server_group(struct conf *cf , struct command *cmd, void *conf)
{
	rstatus_t status;
    struct array *a;
    struct string *value;
    struct conf_server field;
	struct conf_server* subfield;
	struct conf_server_group * group;
    uint8_t *p, *q, *start, *end, *find;
    uint8_t *pname, *addr, *port, *weight, *name;
    uint32_t pnamelen, addrlen, portlen, weightlen, namelen;
	
	memset(&field, 0, sizeof(field));
	
    p = conf;
    a = (struct array *)(p + cmd->offset);

    group = array_push(a);
    if (group == NULL) {
        return CONF_ERROR;
    }
	group->loop = 0;
	status = array_init(&group->server, CONF_DEFAULT_SERVERS,
                        sizeof(struct conf_server));
    if (status != NC_OK) {
        return CONF_ERROR;
    }
	
	value = array_top(&cf->arg);

    
    p = value->data + value->len - 1;
    start = value->data;
    addr = NULL;
    addrlen = 0;
    weight = NULL;
    weightlen = 0;
    port = NULL;
    portlen = 0;
    name = NULL;
    namelen = 0;
	
	while (start < p)
	{
		q = nc_strchr(start, p, ' ');
		if (q)
		{
			end = q;
		}
		else
		{
			end = p+1;
		}
		
		
		find = nc_strchr(start, end, ':');
		if (NULL == find)
		{
			namelen = end - start;
			if (namelen == 4 || nc_strncmp(start, "loop", 4) == 0)
			{
				group->loop = 1;
			}
			break;
		}
		else
		{
			pname = start;
			pnamelen = find - start;
			
			
			status = string_copy(&field.pname, pname, pnamelen);
			if (status != NC_OK) {
				return CONF_ERROR;
			}
			
			addr = pname;
			addrlen = pnamelen;
			
			status = string_copy(&field.name, addr, addrlen);
			if (status != NC_OK) {
				return CONF_ERROR;
			}

			status = string_copy(&field.addrstr, addr, addrlen);
			if (status != NC_OK) {
				return CONF_ERROR;
			}
			
			start = find + 1;
			
			find = nc_strchr(start, end, ':');
			if (NULL == find)
			{
				return CONF_ERROR;
			}
			
			portlen = find - start;
			
			field.port = nc_atoi(start, portlen);
			if (field.port < 0 || !nc_valid_port(field.port)) {
				return "has an invalid port in \"hostname:port:weight\" format string";
			}
			
			start = find + 1;
			
			weightlen = end - start;
			
			field.weight = nc_atoi(start, weightlen);
			if (field.weight < 0) {
				return "has an invalid weight in \"hostname:port:weight\" format string";
			} else if (field.weight == 0) {
				return "has a zero weight in \"hostname:port:weight\" format string";
			}
			
			field.valid = 1;
			subfield = array_push(&group->server);
			if (group == NULL) {
				return CONF_ERROR;
			}
			memcpy(subfield, &field, sizeof(struct conf_server));
			memset(&field, 0, sizeof(field));
		}
		
		if (NULL == q)
		{
			break;
		}
		
		start = q + 1;
		
		while (start < p)
		{
			if (*start == ' ')
			{
				start++;
			}
			else
			{
				break;
			}
		}
	}

    return CONF_OK;
}

char *
conf_add_server(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    struct array *a;
    struct string *value;
    struct conf_server *field;
    uint8_t *p, *q, *start;
    uint8_t *pname, *addr, *port, *weight, *name;
    uint32_t k, delimlen, pnamelen, addrlen, portlen, weightlen, namelen;
    char delim[] = " ::";

    p = conf;
    a = (struct array *)(p + cmd->offset);

    field = array_push(a);
    if (field == NULL) {
        return CONF_ERROR;
    }

    conf_server_init(field);

    value = array_top(&cf->arg);

    /* parse "hostname:port:weight [name]" or "/path/unix_socket:weight [name]" from the end */
    p = value->data + value->len - 1;
    start = value->data;
    addr = NULL;
    addrlen = 0;
    weight = NULL;
    weightlen = 0;
    port = NULL;
    portlen = 0;
    name = NULL;
    namelen = 0;

    delimlen = value->data[0] == '/' ? 2 : 3;

    for (k = 0; k < sizeof(delim); k++) {
        q = nc_strrchr(p, start, delim[k]);
        if (q == NULL) {
            if (k == 0) {
                /*
                 * name in "hostname:port:weight [name]" format string is
                 * optional
                 */
                continue;
            }
            break;
        }

        switch (k) {
        case 0:
            name = q + 1;
            namelen = (uint32_t)(p - name + 1);
            break;

        case 1:
            weight = q + 1;
            weightlen = (uint32_t)(p - weight + 1);
            break;

        case 2:
            port = q + 1;
            portlen = (uint32_t)(p - port + 1);
            break;

        default:
            NOT_REACHED();
        }

        p = q - 1;
    }

    if (k != delimlen) {
        return "has an invalid \"hostname:port:weight [name]\"or \"/path/unix_socket:weight [name]\" format string";
    }

    pname = value->data;
    pnamelen = namelen > 0 ? value->len - (namelen + 1) : value->len;
    status = string_copy(&field->pname, pname, pnamelen);
    if (status != NC_OK) {
        array_pop(a);
        return CONF_ERROR;
    }

    addr = start;
    addrlen = (uint32_t)(p - start + 1);

    field->weight = nc_atoi(weight, weightlen);
    if (field->weight < 0) {
        return "has an invalid weight in \"hostname:port:weight [name]\" format string";
    } else if (field->weight == 0) {
        return "has a zero weight in \"hostname:port:weight [name]\" format string";
    }

    if (value->data[0] != '/') {
        field->port = nc_atoi(port, portlen);
        if (field->port < 0 || !nc_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port:weight [name]\" format string";
        }
    }

    if (name == NULL) {
        /*
         * To maintain backward compatibility with libmemcached, we don't
         * include the port as the part of the input string to the consistent
         * hashing algorithm, when it is equal to 11211.
         */
        if (field->port == CONF_DEFAULT_KETAMA_PORT) {
            name = addr;
            namelen = addrlen;
        } else {
            name = addr;
            namelen = addrlen + 1 + portlen;
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = string_copy(&field->addrstr, addr, addrlen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    /*
     * The address resolution of the backend server hostname is lazy.
     * The resolution occurs when a new connection to the server is
     * created, which could either be the first time or every time
     * the server gets re-added to the pool after an auto ejection
     */

    field->valid = 1;

    return CONF_OK;
}

char *conf_add_write_server(struct conf *cf, struct command *cmd, void *conf)
{
	return conf_add_server(cf, cmd, conf);
}

char *conf_add_backup_server(struct conf *cf, struct command *cmd, void *conf)
{
	return conf_add_server(cf, cmd, conf);
}

char *conf_add_zookeeper_server(struct conf *cf, struct command *cmd, void *conf)
{
    return conf_add_server(cf, cmd, conf);
}

char *
conf_set_num(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int num, *np;
    struct string *value;

    p = conf;
    np = (int *)(p + cmd->offset);

    if (*np != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    num = nc_atoi(value->data, value->len);
    if (num < 0) {
        return "is not a number";
    }

    *np = num;

    return CONF_OK;
}

char *
conf_set_bool(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int *bp;
    struct string *value, true_str, false_str;

    p = conf;
    bp = (int *)(p + cmd->offset);

    if (*bp != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);
    string_set_text(&true_str, "true");
    string_set_text(&false_str, "false");

    if (string_compare(value, &true_str) == 0) {
        *bp = 1;
    } else if (string_compare(value, &false_str) == 0) {
        *bp = 0;
    } else {
        return "is not \"true\" or \"false\"";
    }

    return CONF_OK;
}

char *conf_set_protocol(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    struct string *value;
	struct string redis_str, ssdb_str, memcache_str;
	uint32_t* protocol;

    p = conf;
    protocol = (uint32_t*)(p + cmd->offset);

    if (*protocol != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);
	string_set_text(&redis_str, "redis");
    string_set_text(&ssdb_str, "ssdb");
	string_set_text(&memcache_str, "memcache");
	
	if (string_compare(value, &redis_str) == 0)
	{
		*protocol = PROTOCOL_REDIS;
	}
	else if(string_compare(value, &ssdb_str) == 0)
	{
		*protocol = PROTOCOL_SSDB;
	}
	else if(string_compare(value, &memcache_str) == 0)
	{
		*protocol = PROTOCOL_MEMCACHE;
	}
    else {
        return "protocol error";
    }

    return CONF_OK;
}

char *
conf_set_hash(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    hash_type_t *hp;
    struct string *value, *hash;

    p = conf;
    hp = (hash_type_t *)(p + cmd->offset);

    if (*hp != CONF_UNSET_HASH) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    for (hash = hash_strings; hash->len != 0; hash++) {
        if (string_compare(value, hash) != 0) {
            continue;
        }

        *hp = hash - hash_strings;

        return CONF_OK;
    }

    return "is not a valid hash";
}

char *
conf_set_distribution(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    dist_type_t *dp;
    struct string *value, *dist;

    p = conf;
    dp = (dist_type_t *)(p + cmd->offset);

    if (*dp != CONF_UNSET_DIST) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    for (dist = dist_strings; dist->len != 0; dist++) {
        if (string_compare(value, dist) != 0) {
            continue;
        }

        *dp = dist - dist_strings;

        return CONF_OK;
    }

    return "is not a valid distribution";
}

char *
conf_set_hashtag(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    if (value->len != 2) {
        return "is not a valid hash tag string with two characters";
    }

    status = string_duplicate(field, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}
