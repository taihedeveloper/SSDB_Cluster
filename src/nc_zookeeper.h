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

#ifndef _NC_ZOOKEEPER_H_
#define _NC_ZOOKEEPER_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <zookeeper.h>

#define BUFFER_SIZE 256

struct zk_init_ctx
{
    const char *host;
    int timeout;
    zhandle_t *zh;
};
#ifdef __cplusplus  
extern "C"  {
#endif

zhandle_t *zk_init(const char *host, watcher_fn init_watcher, int timeout, struct zk_init_ctx *zk_ctx); // zk初始化
int zk_close(zhandle_t *zh); // zk关闭连接
int zk_create(zhandle_t *zh, const char *path, const char *value); // zk新建节点
int zk_delete(zhandle_t *zh, const char *path); // zk删除节点
int zk_exists(zhandle_t *zh, const char *path, watcher_fn watcher, void *watcherCtx); // zk确认节点是否存在
int zk_get(zhandle_t *zh, const char *path, watcher_fn watcher, void *watcherCtx, char *buffer, int *buffer_len); // zk获取节点值
int zk_set(zhandle_t *zh, const char *path, const char *buffer); // zk设置节点值
int zk_get_children(zhandle_t *zh, const char *path, watcher_fn watcher, void *watcherCtx, struct String_vector *strings); // zk获取节点的子节点
int comp(const void *a, const void *b); // 排序规则，用于分布式获取锁
bool get_lock(zhandle_t *zh, const char *path, char* node_path); // 获取分布式锁

#ifdef __cplusplus  
}  
#endif  

#endif /* SRC_NC_ZOOKEEPER_H_ */
