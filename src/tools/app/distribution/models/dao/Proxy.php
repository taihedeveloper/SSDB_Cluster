<?php

class Dao_Proxy {
	private $_zk = null;
    public function __construct()
    {
        // $this->_zk = new \Zookeeper(Distribution_Conf::zk_host);
    }   
    
    public function getProxy()
    {
        $ret = file_get_contents('http://192.168.217.12:22222/');
        // var_dump($ret);
        return $ret;
    }
}
