<?php

class Dao_Nodes {
	private $_zk = null;
  public $_code=0;
  public $_msg = "";
    public function __construct()
    {
        $this->_zk = new \Zookeeper(Distribution_Conf::zk_host);
    }
    
    public function getNodes()
    {
        $ret = $this->_zk->getChildren('/nodes');
        if(NULL == $ret)
        {
            $_msg = "get Children fail.";
            return -1;
        }
        for ($i = 0; $i<count($ret); $i++) {
          //echo "$ret[$i] <br>";
          $zk_path = "/nodes/" ."". $ret[$i];
          $json_obj= json_decode( $this->_zk->get($zk_path), true );
          $json_obj["num"] = $ret[$i];
          $result[$i] = $json_obj;
          //echo "json_decode( $result[$i] ) <br>";
        }
        return $result;
    }
    
    public function __getCode(){
      $this->$_code;
    }
    public function __getMsg()
    {
      $this->$_msg;
    }
}
