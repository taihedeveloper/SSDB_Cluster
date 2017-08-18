<?php

class Service_Data_AddNodes {
    private $objDaoSample;
    public function __construct(){
        //$this->objDao= new Dao_AddNodes();
    }
    
    public function addNodes($arrInput)
    {
        if (NULL == $arrInput['masternode'] or NULL == $arrInput['backnode']) {
            var_dump("argv error");
            return -1;
        }

        list($ip, $port) = split (':',$arrInput['masternode']);
        list($slave_ip, $slave_port) = split (':',$arrInput['backnode']);
        $json_path = "./ssdb_cluster_addnode.json";
        $json_str = json_encode(array('ssdb_group' => 
            array(array('ip' => $ip, 'port' => intval($port), 'slave_ip' => $slave_ip, 'slave_port' => intval($slave_port))) ));
        file_put_contents($json_path, $json_str);
        $file_path = Distribution_Conf::ssdb_cluster_init;
        $cmd_str = "/usr/bin/python  " .$file_path ." -a ". $json_path . " -z \"" . Distribution_Conf::zk_host. "\"";
        if(file_exists($file_path)){
            exec($cmd_str, $output, $return_val);
            if ($return_val ) {
                return -1;
            }
            $json_output = json_decode($output[0], true);
            if ( 'failure' == $json_output['action']){
                return $json_output['message'];
            }
        }
        return 0;
    }
}
