#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector> 
#include <fcntl.h>
#include <nc_core.h>
#include <getopt.h>
#include "nc_zookeeper.h"
#include "SSDB_client.h"
#define TIMEOUT 60
#define SPEED 1
struct OBJ
{
	void init()
	{
		zk_host=NULL;
		masternode=NULL;
		backupnode=NULL;
		log_path=NULL;
		daemonize=false;
		slots=NULL;
		ssdb_ip=NULL;
		ssdb_port=0;
		add_node=false;
	}
	char *zk_host;
	char *masternode;
	char *backupnode;
	char *log_path;
	bool daemonize;
	char *slots;
	char *ssdb_ip;
	int ssdb_port;
	bool add_node;
};

static struct option long_options[] = {
    { "daemonize",      no_argument,        NULL,   'd' },
    { "output",         optional_argument,  NULL,   'o' },
    { "masternode",	    required_argument,  NULL,   'm' },
    { "backupnode",	    required_argument,  NULL,   'b' },
    { "zookeeperhost",  required_argument,  NULL,   'z' },
    { "add",     	    no_argument,        NULL,   'a' },
    { "slots",     	    no_argument,        NULL,   's' },
    { "host",     	    no_argument,        NULL,   'h' },
    { "port",     	    no_argument,        NULL,   'p' },
    { NULL,             0,                  NULL,    0  }
};
static char short_options[] = "daAom:b:s:h:p:z:";
void 
help(void){
	log_stderr(
        "Usage: migrate    [-?ad] [-m masterhost]" CRLF
        "                  [-b backupnode] [-z zookeeper host]" CRLF
        "                  [-s slots] [-p ssdb port] [-h ssdb ip]" CRLF
        "                    " CRLF
        "");
	log_stderr(
        "Options:" CRLF
        "  -d, --daemonize        : run as a daemon" CRLF
        "  -a, --add              : add node");
	log_stderr(
		"Examples:" CRLF
		"	migrate slots: migrate -s 1,2,3 -h 127.0.0.1 -p 8888 -z 127.0.0.1:1010" CRLF
		//"	add node: migrate -a -m 127.0.0.1:0001 -b 127.0.0.1:0002 -z 127.0.0.1:1010" CRLF
		"");
	return ;
}
int 
daemonize(const char *dir=NULL){
	switch(fork()){
		case -1:
			return -1;
		case 0:
			break;
		default:
			exit(0);
	}
	if(setsid() == -1){
		exit(0);
	}
	if(dir != NULL){
		if(chdir(dir) == -1){
			exit(0);
		}
	}

	if(close(STDIN_FILENO) == -1){
		exit(0);
	}
	if(close(STDOUT_FILENO) == -1){
		exit(0);
	}
	if(close(STDERR_FILENO) == -1){
		exit(0);
	}

	int fd = open("/dev/null", O_RDWR, 0);
	if(fd == -1){
		exit(0);
	}
	if(dup2(fd, STDIN_FILENO) == -1){
		exit(0);
	}
	if(dup2(fd, STDOUT_FILENO) == -1){
		exit(0);
	}
	if(dup2(fd, STDERR_FILENO) == -1){
		exit(0);
	}
	return 0;
}
void 
split(const char *str, const char *key, std::vector<std::string> &v){
	char *p=(char* )str;
	char val[16] ="\0";
	int i=0;
	for(;true;p++)
	{
		if ( *p == (long)key )
		{
			if ( strlen(val) > 0 )
				v.push_back( (val) );
			i=0;
			memset(val, 0x00, sizeof(val));
			continue;
		}
		val[i++] = *p;
		if( *p == '\0' )
		{
			if (strlen(val) > 0 )
				v.push_back( (val) );
			i=0;
			memset(val, 0x00, sizeof(val));
			break;
		}
	}
	return ;
}
int get_options(int argc, char **argv, struct OBJ *migrate){
	int c;
	migrate->add_node = false;
	migrate->daemonize = false;
	for (;;) {

		c = getopt_long(argc, argv, short_options,
                long_options, NULL);
		if (c == -1)
           break;
		switch (c) {

			case 'd':
				migrate->daemonize = true;
				break;
			case 'a':
				migrate->add_node = true;
				break;
			case 's':
				if( 45 == *optarg ) return -1;
				migrate->slots = optarg;
				break;
			case 'h':
				if( 45 == *optarg ) return -1;
				migrate->ssdb_ip = optarg;
				break;
			case 'p':
				if( 45 == *optarg ) return -1;
				migrate->ssdb_port = atoi(optarg);
				break;
			case 'm':				
				if( 45 == *optarg ) return -1;
				migrate->masternode = optarg;
				break;
			case 'b':
				if( 45 == *optarg ) return -1;
				migrate->backupnode = optarg;
				break;
			case 'o':
				if( 45 == *optarg ) return -1;
				migrate->backupnode = optarg;
				break;
			case 'z':
				if( 45 == *optarg ) return -1;
				migrate->zk_host = optarg;
				break;
			// case '?':
			// 	return -1;
           		//break;

       		default:
           		//printf("?? getopt returned character code 0%o ??\n", c);
           		return -1;
		}

	}
   return 0;
}
void 
migarte_status(const zhandle_t *zh_handler, int nodes_num, const char *status,const std::vector<std::string> &vecSlots){
	zhandle_t * zk_handler = (zhandle_t *)zh_handler;
	char zk_path[128] = {0};
	char data[256] = {0};
	int count = vecSlots.size();
	for (int i = 0; i < count;i++)
	{
		memset(zk_path, 0x00, sizeof(zk_path));
		memset(data, 0x00, sizeof(data));
		sprintf(zk_path, "/migrate_tasks/%s", vecSlots[i].c_str());
		sprintf(data,"{\"node_index\":%d, \"slots\":%s, \"migrating\":\"true\"}",nodes_num, vecSlots[i].c_str(), status);
		int ret = zk_create(zk_handler, zk_path, data);
		if (ret)
		{
			_log_stderr("zk_create %s fail .\n",zk_path);
			continue;
		}
	}
	return ;
}

int 
add_node(const zhandle_t *zh_handler, const char *masternode, const char *backupnode=NULL){
	std::vector<std::string> vecMnode;
	split(masternode, (char *)':',  vecMnode);
	const char *ip = vecMnode[0].c_str();
	int port = atoi(vecMnode[1].c_str());
	zhandle_t * zk_handler = (zhandle_t *)zh_handler;
	if (zh_handler){
		struct String_vector strings;
	    int child_ret = zk_get_children(zk_handler, "/nodes", NULL, NULL, &strings);
	    if (child_ret) {
	    	log_error("get /nodes children fail.\n");
	        return -1;
	    }
	    //添加排序
	    qsort(strings.data, strings.count, sizeof(char *), comp);
	    int nodes_num = strings.count;
	    //查找节点
	    for (int i = 0; i < nodes_num; ++i)
	    {
	    	char data[512] ={0};
	    	int datalen = sizeof(data);
	    	char zk_path[256] = {0};

            sprintf(zk_path, "/nodes/%s", strings.data[i] );
            int ret = zk_get(zk_handler, ((const char *)zk_path), NULL, NULL, data, &datalen);
            //printf("ret:::::::::::::::::%d\n",ret );
            if (!ret) {
            	//printf(" data:%s\n",data);
                //解析data获取ip&端口
                char server_ip[64] = {0};
                int server_port=0;
                const char *tmp_ip=NULL;
                json_object *json_data = json_tokener_parse(data);
                JSON_GET_STR(tmp_ip, json_data, "ip", server_ip);
                JSON_GET_INT32(json_data, "port", &server_port, 0);
                //printf(" port:%d, server_port:%d, ip:%s ,server_ip:%s\n",port, server_port, ip, server_ip);
                if ( port == server_port && strcmp(server_ip, ip)==0 )
                	return atoi(strings.data[i]);
            }
            else {
            	log_error("zk_get fail\n");
            	return -1;
            }
	    }
	    if (backupnode != NULL)
	    {

			std::vector<std::string> vecBnode;
			split(backupnode, (char *)':',  vecBnode);
		    char data[1024] = {0};
		    char zk_path[64] ={0};
		    sprintf(zk_path,"/nodes/%d",atoi(strings.data[nodes_num-1]) + 1 );
		    sprintf(data,"{\"Status\":0, \"ip\":\"%s\", \"port\": %d , \"slave_ip\":\"%s\", \"slave_port\": %s }", 
		    	ip, port , vecBnode[0].c_str(), vecBnode[1].c_str());
		    int ret=zk_create(zk_handler, zk_path, data);
		    if (ret)
		    {
		    	return -1;
		    }
		    printf("add node success ,zk_path:%s\n", zk_path);
		    return atoi(strings.data[nodes_num-1]) + 1;
	    }
	}
	return -1;
}

static struct logger logger;
int main(int argc, char  **argv)
{
	if (argc < 6){
	 	help();
	 	exit(0);
	 }
	struct OBJ migrate;
	migrate.init();
	int t = get_options(argc, argv, &migrate);
	if (t != 0)
	{
		help();
		exit(0);
	}
    int status = log_init(5, (char *)"./migrate.log");
    if (status != 0) {
    	log_warn("init log fail.");
        return -1;
    }
    if (migrate.daemonize)
	{
		daemonize();
	}
	const char *slots = migrate.slots; // argv[1];
	const char *ip =  migrate.ssdb_ip;//argv[2];
	int port =  migrate.ssdb_port;//atoi(argv[3]);	
	// 连接 zk
	char host[128];
    zhandle_t *zh_handler = zk_init(migrate.zk_host, NULL, 30000, NULL); // 需改
    if (!zh_handler) {
        log_error("connect to zk fail.");
        exit(0);
    }
    _log_stderr("connect to zk success.\n");
 	if (migrate.masternode == NULL) sprintf(host,"%s:%d",ip, port);
    else strcpy(host,migrate.masternode);
    //
    int nodes_num = add_node(zh_handler, host, migrate.backupnode);
    if (nodes_num < 0 )
    {
    	log_error("add_node fail.");
    	exit(0);
    }
    if ( migrate.add_node )
    {
    	_log_stderr("add node success,nodes_num:%d\n",nodes_num);
    	exit(0);
    }
    _log_stderr("nodes_num:%d\n",nodes_num );
    


    std::vector<std::string> vecSlots;
	split(slots, (char *)',',  vecSlots);
	migarte_status(zh_handler, nodes_num, "true",  vecSlots);
    char data[1024];
    int datalen = sizeof(data);
    char zk_path[50];
    int pathlen = sizeof(zk_path);

    ssdb::Client *dest_client = ssdb::Client::connect(ip, port);
	if(dest_client == NULL){
		log_error("fail to connect to server. ip:%s , port:%d",ip, port);
		return 0;
	}
	_log_stderr("connect to dest_server success.\n");
	std::map<char *, ssdb::Client *> mapServerPool;
	for (std::vector<std::string>::iterator it = vecSlots.begin() ; it != vecSlots.end(); it++ )
	{
		//get slot -> node  (ip,port)
		char *src_ip = NULL;
		int src_port = 0;
		char slot_map[1024] = {0};
		if(zh_handler)
		{
			int node_index=-1;
			memset(data,0x00, sizeof(data));
            memset(zk_path,0x00, sizeof(pathlen));
            sprintf(zk_path, "/slot_map/%s", (*it).c_str() );
            int get_ret = zk_get(zh_handler, (zk_path), NULL, NULL, data, &datalen);
            if (!get_ret) {
                //解析data 获取nodes index
                //_log_stderr("data:%s\n",data );
                json_object *json_data = json_tokener_parse(data);
                JSON_GET_INT32(json_data, "node_index", &node_index, 0);
            } 
            else {
            	log_error("get slot_map fail,zk_path:%s .", zk_path);
            	continue;
            	//获取失败
            }
            if(node_index < 0 )
            {
            	log_error("get node_index fail ,node_index:%d .", node_index);
            	continue;
            }

            _log_stderr("node_index:%d\n", node_index);
            //根据index获取node信息
            datalen = sizeof(data);
            memset(data,0x00, sizeof(data));
            memset(zk_path,0x00, sizeof(pathlen));
            sprintf(zk_path, "/nodes/%d", node_index );
            get_ret = zk_get(zh_handler, ((const char *)zk_path), NULL, NULL, data, &datalen);
            if (!get_ret) {
                //解析data获取ip&端口
                //printf("data:%s\n",data );
                char server_ip[64] = {0};
                const char *tmp_ip=NULL;
                json_object *json_data = json_tokener_parse(data);
                JSON_GET_STR(tmp_ip, json_data, "ip", server_ip);
                src_ip = server_ip;
                JSON_GET_INT32(json_data, "port", &src_port, 0);

            } 
            else {
            	//获取失败
            	log_error("get slot fail,zk_path:%s .", zk_path);
            	continue;
            }
            
		}
		
		std::map<char *, ssdb::Client *>::iterator iter;
		iter = mapServerPool.find(src_ip);
		if ( iter != mapServerPool.end() )  //找到句柄
		{
			ssdb::Status s;
			s = iter->second->slot_premigrating(*it);
			if(s.ok()){
				_log_stderr("ssdb slot_premigrating cmd ok!\n");
			}else{
				log_error("slot_premigrating error!\n");
            	//continue;
			}
			s= dest_client->slot_preimporting(*it);
			if(s.ok()){
				_log_stderr("ssdb slot_preimporting cmd ok!\n");
			}else{
				log_error("slot_preimporting error!\n");
            	//continue;
			}

			s = iter->second->migrate_slot(*it, ip, port, TIMEOUT, SPEED);
			if(s.ok()){
				_log_stderr("ssdb migrate_slot cmd ok!\n");
			}else{
				log_error("migrate_slot error!\n");
            	//continue;
			}
			s = iter->second->slot_postmigrating(*it);
			if(s.ok()){
				_log_stderr("ssdb slot_postmigrating cmd ok!\n");
			}else{
				log_error("slot_postmigrating error!\n");
            	//continue;
			}

			s= dest_client->slot_postimporting(*it);
			if(s.ok()){
				printf("ssdb slot_postimporting cmd ok!\n");
			}else{
				log_error("slot_postimporting error!\n");
            	//continue;
			}
		}
		else 
		{
			ssdb::Client *client = ssdb::Client::connect(src_ip, src_port);
			if(client == NULL){
				log_error("fail to connect to src_server!\n");
            	//continue;
			}
			ssdb::Status s;
			s = client->slot_premigrating(*it);
			if(s.ok()){
				_log_stderr("ssdb slot_premigrating cmd ok!\n");
			}else{
				log_error("slot_premigrating error!\n");
            	//continue;
			}
			s= dest_client->slot_preimporting(*it);
			if(s.ok()){
				_log_stderr("ssdb slot_preimporting cmd ok!\n");
			}else{
				log_error("slot_preimporting error!\n");
            	//continue;
			}
			s = client->migrate_slot(*it, ip, port, TIMEOUT, SPEED);
			if(s.ok()){
				_log_stderr("ssdb migrate_slot cmd ok!\n");
			}else{
				log_error("migrate_slot error!\n");
            	//continue;
			}
			s = client->slot_postmigrating(*it);
			if(s.ok()){
				_log_stderr("ssdb slot_postmigrating cmd ok!\n");
			}else{
				log_error("slot_postmigrating error!\n");
            	//continue;
			}
			s= dest_client->slot_postimporting(*it);
			if(s.ok()){
				_log_stderr("ssdb slot_postimporting cmd ok!\n");
			}else{
				log_error("slot_postimporting error!\n");
            	//continue;
			}
			mapServerPool[src_ip]=client;
		}
		//update zk slot->node 迁移完成修改slot信息
		memset(data,0x00, sizeof(data));
		memset(zk_path,0x00, sizeof(pathlen));
		sprintf(zk_path, "/slot_map/%s", (*it).c_str()  );
		printf("zk_path: %s\n", zk_path);
		sprintf(data,"{\"node_index\":%d, \"migrating\":\"false\"}",nodes_num);
		if (zk_set(zh_handler, zk_path, data))
			log_error("zk_set fail ,zk_path:%s, data:%s",zk_path, data);
		memset(zk_path, 0x00, sizeof(zk_path));
		sprintf(zk_path, "/migrate_tasks/%s", (*it).c_str());
		if(zk_delete(zh_handler,zk_path))
			log_error("zk_del fail ,zk_path:%s",zk_path);
		it=vecSlots.erase(it);
		it--;
	}
	std::map<char *, ssdb::Client *>::iterator iter;
	for (iter = mapServerPool.begin(); iter != mapServerPool.end(); )
	{
		if(iter->second != NULL) 
			delete iter->second;
		mapServerPool.erase(iter++);
	}
	if (dest_client != NULL) delete dest_client;
	if (zh_handler) zk_close(zh_handler);

	return 0;
}