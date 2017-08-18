<?php

class Dao_Slots {
	private $_zk = null;
    public function __construct()
    {
        $this->_zk = new \Zookeeper(Distribution_Conf::zk_host);
    }   
    
    public function getSlots()
    {

        // $ret = $this->_zk->getChildren('/slot_map');
        // for ($i = 0; $i<count($ret) ; $i++) {
        //   //echo "$ret[$i] <br>";
        //   $zk_path = "/slot_map/" ."". $ret[$i];
        //   $result[$i] = json_decode($this->_zk->get($zk_path));
        //   //$json_obj["number"] = $i;
        //   //$result[$i] = $json_obj;
        //   //echo "json_decode( $result[$i] ) <br>";
        // }

      $file_path = Distribution_Conf::slot_file_path;
      if(file_exists($file_path)){
        $file_arr = file($file_path);
        for($i=0;$i<count($file_arr);$i++){//逐行读取文件内容
          $result[$i] = json_decode($file_arr[$i]);
          }
        }
        return $result;
    }
}
