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
#include "nc_zookeeper.h"
#include <nc_core.h>
#include <nc_server.h>

zhandle_t *zk_init(const char *host, watcher_fn init_watcher, int timeout, struct zk_init_ctx *zk_ctx)
{
    zhandle_t *zkhandle = zookeeper_init(host, init_watcher, timeout, 0, zk_ctx, 0);
    if (!zkhandle) {
        log_error("Init zookeeper Conn Fail");
        return NULL;
    }
    return zkhandle;
}

int zk_close(zhandle_t *zh)
{
    return zookeeper_close(zh);
}

int zk_create(zhandle_t *zh, const char *path, const char *value)
{
    size_t valuelen = strlen(value);
    int ret = zoo_create(zh, path, value, (int)valuelen, &ZOO_OPEN_ACL_UNSAFE, 0,
            NULL, 0);
    if (ret) {
        log_error("Error %d for create\n", ret);
    }
    return ret;
}

int zk_delete(zhandle_t *zh, const char *path)
{
    int ret = zoo_delete(zh, path, -1);
    if (ret) {
        log_error("Error %d for delete\n", ret);
    }
    return ret;
}

int zk_exists(zhandle_t *zh, const char *path, watcher_fn watcher,
        void *watcherCtx)
{
    int ret = zoo_wexists(zh, path, watcher, watcherCtx, NULL);
    if (ret) {
        log_error("Error %d for wexists\n", ret);
    }
    return ret;
}

int zk_get(zhandle_t *zh, const char *path, watcher_fn watcher, void *watcherCtx, 
        char *buffer, int *buffer_len)
{
    int ret = zoo_wget(zh, path, watcher, watcherCtx, buffer, buffer_len,
            NULL);
    if (ret) {
        log_error("Error %d for wget\n", ret);
    }

    return ret;
}

int zk_set(zhandle_t *zh, const char *path, const char *buffer)
{
    size_t buflen = strlen(buffer);
    int ret = zoo_set(zh, path, buffer, (int)buflen, -1);
    if (ret) {
        log_error("Error %d for set\n", ret);
    }
    return ret;
}

int zk_get_children(zhandle_t *zh, const char *path, watcher_fn watcher,
        void *watcherCtx, struct String_vector *strings)
{
    int ret = zoo_wget_children(zh, path, watcher, watcherCtx, strings);
    if (ret) {
        log_error("Error %d for wget_children\n", ret);
    }
    return ret;
}

int comp(const void *a, const void *b)
{
    return atoi(*(char **)a) > atoi(*(char **)b);
}

bool get_lock(zhandle_t *zh, const char *path, char* node_path)
{
    char total_path[BUFFER_SIZE];
    size_t total_len = sizeof(total_path);
    memset(total_path, 0, total_len);
    strcat(total_path, "/lock/");
    strcat(total_path, path);

    char path_buffer[BUFFER_SIZE];
    size_t buffer_len = sizeof(path_buffer);
    memset(path_buffer, 0, buffer_len);

    // create father node
    int father_ret = zoo_create(zh, total_path, "0", 1, &ZOO_OPEN_ACL_UNSAFE, 0,
            NULL, 0);
    if (father_ret == 0 || father_ret == -110) {
        char create_path[BUFFER_SIZE];
        memset(create_path, 0, BUFFER_SIZE);
        strcat(create_path, total_path);
        strcat(create_path, "/");
        int create_ret = zoo_create(zh, create_path, "0", 1, &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE,
                path_buffer, (int)buffer_len);
        if (create_ret) {
            log_error("Error %d for create\n", create_ret);
            return false;
        }

        while (1) {
            struct String_vector strings;
            int child_ret = zoo_wget_children(zh, total_path, NULL, NULL, &strings);
            if (child_ret) {
                log_error("Error %d for wget_children\n", child_ret);
                return false;
            }

            if (strings.count <= 0) {
                return false;
            }
            if (strings.count == 1) {
                strcpy(node_path, path_buffer);
                return true;
            }
            else {
                qsort(strings.data, (size_t)strings.count, sizeof(char *), comp);
                char ab_path[BUFFER_SIZE];
                memset(ab_path, 0, BUFFER_SIZE);
                strcat(ab_path, total_path);
                strcat(ab_path, "/");
                strcat(ab_path, strings.data[0]);
                if (strcmp(ab_path, path_buffer) == 0) {
                    strcpy(node_path, path_buffer);
                    return true;
                }
                else {
                    int32_t strings_index = 1;
                    for (;strings_index < strings.count; ++strings_index) {
                        char check_path[BUFFER_SIZE];
                        memset(check_path, 0, BUFFER_SIZE);
                        strcat(check_path, total_path);
                        strcat(check_path, "/");
                        strcat(check_path, strings.data[strings_index]);
                        if (strcmp(check_path, path_buffer) == 0) {
                            break;
                        }
                    }
                    char child_path[BUFFER_SIZE];
                    memset(child_path, 0, BUFFER_SIZE);
                    strcat(child_path, total_path);
                    strcat(child_path, "/");
                    strcat(child_path, strings.data[strings_index - 1]);
                    int exists_ret = zk_exists(zh, child_path, NULL, NULL);
                    switch (exists_ret) {
                        case 0:
                            usleep(1000);
                            continue;
                        case -101:
                            strcpy(node_path, path_buffer);
                            return true;
                        default:
                            log_error("Error %d for zk_exists\n", zk_exists);
                            return false;
                    }
                }
            }
        }
    }
    else {
        log_error("Error %d for create\n", father_ret);
        return false;
    }
}