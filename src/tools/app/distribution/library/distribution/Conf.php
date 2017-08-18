<?php
/**
 * @file Conf.php
 * @author luohongcang@taihe.com
 *  
 **/
 
class Distribution_Conf
{
    const SUCCESS = 22000;
    const PARAM_ERROR = 22005;
    const FAILED = 22001;
	const UNLOGIN = 22452;
	const NOPERMISSION = 22009;
    const OPERATEPERMISSION = 22008;
    const NODE_EXIST=22009; //节点已存在
    
    //config
    const zk_host = "192.168.217.11:2181";
    const slot_file_path = "/home/wangchangqing/slotmap";
	const twemproxy_stat_url = "http://192.168.217.12:22222";
	const migrate_bin_path = "/home/wangchangqing/ssdb_dev/twemproxy/src/tools/migrate";
	const ssdb_cluster_init = "/home/wangchangqing/ssdb_dev/twemproxy/src/tools/ssdb_cluster_init/ssdb_cluster_init.py";
}