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

#include <stdio.h>
#include <stdlib.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_hashkit.h>
#include <nc_zookeeper.h>

#define HASHSLOT_CONTINUUM_ADDITION   10  /* # extra slots to build into continuum */
#define HASHSLOT_POINTS_PER_SERVER    1

void hashslot_get_watch(zhandle_t *zh, int type, int state, const char *path,
        void *watcherCtx)
{
    if (type == ZOO_CHANGED_EVENT || type == ZOO_DELETED_EVENT) {
        struct slot_ctx *ctx_temp = (struct slot_ctx *)watcherCtx;
        int slot_index = ctx_temp->slot_index;
        struct server_pool *pool = ctx_temp->pool;
        char data[128];
        int datalen = sizeof(data);
        memset(data, 0, datalen);
        int get_ret = zk_get(zh, path, hashslot_get_watch, watcherCtx, data, &datalen);
        if (get_ret) {
            return;
        }

        json_object *json_data = json_tokener_parse(data);
        int node_index;
        JSON_GET_INT32(json_data, "node_index", &node_index, 0);
        json_object_put(json_data);
        uint32_t old_index = pool->hashslot[slot_index].index;
        __sync_bool_compare_and_swap(&(pool->hashslot[slot_index].index), old_index, node_index);
    } else {
        return;
    }
}

static rstatus_t
hashslot_init(struct server_pool *pool, uint32_t nserver){
    struct continuum *hashslot;
    uint32_t server_index;        /* server index */
    uint32_t slot_index;          /* slot index */
    uint32_t server_slot_per_num; /* server have slot number 8*/
    struct slot_ctx *ctx_temp;


    hashslot = nc_realloc(pool->hashslot, sizeof(*hashslot) * HASHSLOT_SLOT_NUM);
    if(hashslot == NULL) {
        return NC_ENOMEM;
    }

    pool->hashslot = hashslot;
    pool->nhashslotnum = HASHSLOT_SLOT_NUM;

    int zk_check_flag = 1;

    if (!pool->zh_handler) {
        zk_check_flag = 0;
    } else {
        struct String_vector strings;
        int child_ret = zk_get_children(pool->zh_handler, "/slot_map", NULL, NULL, &strings);
        if (child_ret != 0) {
            zk_check_flag = 0;
        } else {
            int slot_num = strings.count;
            if (slot_num != HASHSLOT_SLOT_NUM) {
                zk_check_flag = 0;
            }
        }
    }

    server_slot_per_num = pool->nhashslotnum / nserver;
    char data[128];
    int datalen = sizeof(data);
    memset(data, 0, datalen);
    char zk_path[50];
    int pathlen = sizeof(zk_path);
    memset(zk_path, 0, pathlen);
    for (slot_index = 0; slot_index < HASHSLOT_SLOT_NUM; slot_index++) {
        server_index = slot_index / server_slot_per_num;

        //Handle the remainder
        if(server_index == nserver) {
            server_index = nserver - 1;
        }

        if (zk_check_flag) {
            memset(data, 0, datalen);
            memset(zk_path, 0, pathlen);
            sprintf(zk_path, "/slot_map/%d", slot_index);
            ctx_temp = array_get(&pool->ctx_array, slot_index);
            ctx_temp->slot_index = slot_index;
            ctx_temp->pool = pool;
            int get_ret = zk_get(pool->zh_handler, ((const char *)zk_path),  hashslot_get_watch, ctx_temp, data, &datalen);
            if (get_ret) {
                pool->hashslot[slot_index].index = server_index;
            } else {
                json_object *json_data = json_tokener_parse(data);
                int node_index;
                JSON_GET_INT32(json_data, "node_index", &node_index, 0);
                json_object_put(json_data);
                pool->hashslot[slot_index].index = node_index;
            }
        } else {
            pool->hashslot[slot_index].index = server_index;
        }
        pool->hashslot[slot_index].value = 0;
    }
    return NC_OK;
}

rstatus_t
hashslot_update(struct server_pool *pool)
{
    uint32_t nserver;             /* # server - live and dead */
    uint32_t nlive_server;        /* # live server */
    uint32_t server_index;        /* server index */
    uint32_t slot_index;          /* slot index */
    uint32_t total_weight;        /* total live server weight */
    uint32_t server_slot_per_num; /* server have slot number 8*/
    int64_t now;                  /* current timestamp in usec */
    struct slot_ctx *ctx_temp;
    struct server *backup_server, *server, tmp_server;

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    nserver = array_n(&pool->server);
    nlive_server = 0;
    total_weight = 0;
    pool->next_rebuild = 0LL;

    struct String_vector strings;
    int child_ret = zk_get_children(pool->zh_handler, "/nodes", NULL, NULL, &strings);
    if (child_ret) {
        return NC_ERROR;
    } else {
        qsort(strings.data, (size_t)strings.count, sizeof(char *), comp);
    }
    char zk_path[50];
    char back_ip_str[128], server_ip_str[128];
    char zk_set_str[256];
    for (server_index = 0; server_index < nserver; server_index++) {
        server = array_get(&pool->server, server_index);
        if(server->next_retry > now) {
            backup_server = array_get(&pool->backup_server, server_index);
            memcpy(&tmp_server, backup_server, sizeof(struct server));
            memcpy(backup_server, server, sizeof(struct server));
            memcpy(server, &tmp_server, sizeof(struct server));
            
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

            log_warn("switch between master and slave machines, now master:pool "
                    "%"PRIu32" '%.*s' server address:%.*s",pool->idx,
                    pool->name.len, pool->name.data, server->addrstr.len, server->addrstr.data);
        }

        if (pool->auto_eject_hosts) {
            if (server->next_retry <= now) {
                server->next_retry = 0LL;
                nlive_server++;
            } else if (pool->next_rebuild == 0LL ||
                       server->next_retry < pool->next_rebuild) {
                pool->next_rebuild = server->next_retry;
            }
        } else {
            nlive_server++;
        }

        ASSERT(server->weight > 0);

        /* count weight only for live servers */
        if (!pool->auto_eject_hosts || server->next_retry <= now) {
            total_weight += server->weight;
        }
    }

    pool->nlive_server = nlive_server;

    if (nlive_server == 0) {
        ASSERT(pool->continuum != NULL);
        ASSERT(pool->ncontinuum != 0);

        log_debug(LOG_DEBUG, "no live servers for pool %"PRIu32" '%.*s'",
                  pool->idx, pool->name.len, pool->name.data);

        return NC_OK;
    }
    log_debug(LOG_DEBUG, "%"PRIu32" of %"PRIu32" servers are live for pool "
              "%"PRIu32" '%.*s'", nlive_server, nserver, pool->idx,
              pool->name.len, pool->name.data);

    /*
     * Allocate the continuum for the pool, the first time, and every time we
     * add a new server to the pool
     */
    if (pool->hashslot == NULL) {
        hashslot_init(pool, nserver);
    }

    return NC_OK;
}

uint32_t
hashslot_dispatch(struct continuum *hashslot, uint32_t nhashslotnum, uint32_t hash)
{
    struct continuum *c;

    ASSERT(continuum != NULL);
    ASSERT(ncontinuum != 0);

    c = hashslot + hash % nhashslotnum;

    return c->index;
}
