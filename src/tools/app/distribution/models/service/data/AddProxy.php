<?php

class Service_Data_AddProxy {
    private $objDaoSample;
    public function __construct(){
        //$this->objDao= new Dao_AddProxy();
    }
    
    public function addProxy($arrInput, &$data)
    {
        if (NULL == $arrInput['ip'] or NULL == $arrInput['port']) {
            var_dump("argv error");
            return -1;
        }

	$ip = $arrInput['ip'];
	$port = $arrInput['port'];

	for ($i = 0; $i<2; $i++){
            $socket = fsockopen($ip, $port, $errno, $errstr, 1);
	    if ($errno == 0) break;
            if (($errno != 0) && $i==1){
		$data = "the ip and port can not be accessed";
		return -1;
            }
	}

        $file_path = Distribution_Conf::ssdb_cluster_init
        $cmd_str = "/usr/bin/python  " .$file_path ." -m add_twemproxy -t " .$ip ." -p" .$port ." -z \"" . Distribution_Conf::zk_host. "\"";

        if(file_exists($file_path)){
            exec($cmd_str, $output, $return_val);
            if ($return_val ) {
                return -1;
            }
            $json_output = json_decode($output[0], true);
            if ( 'failure' == $json_output['action']){
		$data = $json_output['message'];
		return -1;
            }
        }

        return 0;
    }
}
