<?php

class Dao_Proxy {
	private $_zk = null;
    public function __construct()
    {
        $this->_zk = new \Zookeeper(Distribution_Conf::zk_host);
    }   
 
    public function getProxy()
    {
        $ret = $this->_zk->getChildren('/proxy');
        for ($i = 0; $i<count($ret); $i++) {
          $zk_path = "/proxy/" ."". $ret[$i];
          $json_obj= json_decode( $this->_zk->get($zk_path), true );
          $json_obj["num"] = $ret[$i];
          $result[$i] = $json_obj;
        }
        return $result;
    }
}
