<?php
/**
 * @name Dao_Sample
 * @desc sample dao, 可以访问数据库，文件，其它系统等
 * @author luohongcang@taihe.com
 */
class Dao_Sample {
	private $_zk = null;
    public function __construct()
    {
        $this->_zk = new \Zookeeper(Distribution_Conf::zk_host);
    }   
    
    public function getTest()
    {
        $ret = $this->_zk->get('/test');
        return $ret;
    }
}
