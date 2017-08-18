//============================================================================
// Name        : ssdb_test.cpp
// Author      : dexter
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include <iostream>
#include <vector>
#include <string>

#include <stdlib.h>

#include "nc_master_slave.h"
#include "SSDB_client.h"


int active_standby_switch(const char* ip, uint16_t port, uint64_t* last_seq)
{
    ssdb::Client *dest_client = ssdb::Client::connect(ip, port);
    if(dest_client == NULL) return SSDB_CONNECTED_ERROR;

    std::vector<std::string> vct;
    ssdb::Status s = dest_client->show_slave_status(&vct);
    uint64_t last_key = 0ULL;


    if(!s.ok())
    {
        if(dest_client)
        {
            delete dest_client;
            dest_client = NULL;
        }
        return SSDB_SHOW_SLAVE_STATUS_ERROR;
    }

    uint64_t running  = 0ULL;

    for(int i = 0; i < vct.size();)
    {
        if("running" == vct[i])
        {
            running = atoi(vct[i+1].c_str());
            i+=2;
            continue;
        }

        if("last-seq" == vct[i])
        {
            last_key = atol(vct[i+1].c_str());
            i+=2;
            continue;
        }

        if("binlog-seq" == vct[i])
        {
            *last_seq = atol(vct[i+1].c_str());
            i+=2;
            continue;
        }
        i++;
    }

    if(running > 0)
    {
        ssdb::Status s = dest_client->stop_slave();
        if(!s.ok())
        {
            if(dest_client)
            {
                delete dest_client;
                dest_client = NULL;
            }
            return SSDB_STOP_SLAVE_ERROR;
        }
    }

    vct.clear();
    s = dest_client->change_master_to("", 0, last_key, "", &vct);
    if(!s.ok())
    {
        if(dest_client)
        {
            delete dest_client;
            dest_client = NULL;
        }
        return SSDB_CHANGE_MASTER_TO_ERROR;
    }

    s = dest_client->unlock_db();
    if(!s.ok())
    {
        if(dest_client)
        {
            delete dest_client;
            dest_client = NULL;
        }
        return SSDB_UNLOCK_DB_ERROR;
    }

    delete dest_client;
    dest_client = NULL;
    return SSDB_SUCCESS;
}

int change_master_to(const char* ip, uint16_t port, uint64_t last_seq, const char *master_ip, uint16_t master_port)
{
    ssdb::Client *dest_client = ssdb::Client::connect(ip, port);
    if(dest_client == NULL) return SSDB_CONNECTED_ERROR;

    std::vector<std::string> vct;
    ssdb::Status s = dest_client->show_slave_status(&vct);

    if(!s.ok())
    {
        if(dest_client)
        {
            delete dest_client;
            dest_client = NULL;
        }
        return SSDB_SHOW_SLAVE_STATUS_ERROR;
    }

    uint64_t running  = 0ULL;

    for(int i = 0; i < vct.size();)
    {
        if("running" == vct[i])
        {
            running = atoi(vct[i+1].c_str());
            i+=2;
            continue;
        }
        i++;
    }

    if(running > 0) return SSDB_SLAVE_RUNNING_ERROR;

    vct.clear();
    s = dest_client->change_master_to(master_ip, master_port, last_seq, "", &vct);
    if(!s.ok())
    {
        if(dest_client)
        {
            delete dest_client;
            dest_client = NULL;
        }
        return SSDB_CHANGE_MASTER_TO_ERROR;
    }

    s = dest_client->start_slave();
    if(!s.ok())
    {
        if(dest_client)
        {
            delete dest_client;
            dest_client = NULL;
        }
        return SSDB_START_SLAVE_ERROR;
    }

    delete dest_client;
    dest_client = NULL;
    return SSDB_SUCCESS;
}
