/*
 * master_slave.h
 *
 *  Created on: Jul 31, 2017
 *      Author: ZHANGPENG
 */

#ifndef NC_MASTER_SLAVE_H_
#define NC_MASTER_SLAVE_H_

#include <stdint.h>

#define SSDB_SUCCESS                   0
#define SSDB_CONNECTED_ERROR          -1
#define SSDB_SHOW_SLAVE_STATUS_ERROR  -2
#define SSDB_START_SLAVE_ERROR        -3
#define SSDB_STOP_SLAVE_ERROR         -4
#define SSDB_CHANGE_MASTER_TO_ERROR   -5
#define SSDB_UNLOCK_DB_ERROR          -6
#define SSDB_SLAVE_RUNNING_ERROR      -7


#ifdef __cplusplus
extern "C"
{
#endif
    int active_standby_switch(const char* ip, uint16_t port, uint64_t* last_seq);
    int change_master_to(const char* ip, uint16_t port, uint64_t last_seq, const char* master_ip, uint16_t master_port);
#ifdef __cplusplus
}
#endif

#endif /* NC_MASTER_SLAVE_H_ */
