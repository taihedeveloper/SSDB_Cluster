<?php

class Dao_MemInfo {
    public function __construct()
    {
    }   
    
    public function getMemInfo()
    {

      $file_path = "/home/wangchangqing/tools/ssdb_cluster_init/ssdb_cluster_init.py";
      $zookeeper_str = "192.168.217.11:2181";
      if(file_exists($file_path)){
          $cmd_str = "/usr/bin/python " . $file_path . " -z " . $zookeeper_str . " -m ssdb_mem_info 2>error.log";
          exec($cmd_str, $out);
          return json_decode($out[0]);
      }
      return "Can not found tools";
    }
}
