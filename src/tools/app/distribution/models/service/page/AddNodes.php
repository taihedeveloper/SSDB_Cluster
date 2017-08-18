<?php

class Service_Page_AddNodes {
    private $objServiceDataNodes;
    public function __construct(){
        $this->objServiceDataNodes = new Service_Data_AddNodes();
    }

    public function execute($arrInput){
    	// var_dump($arrInput);
    	$ret = $this->objServiceDataNodes->addNodes($arrInput);
    	if(0 != $ret){
    		return array('error_code' => Distribution_Conf::SUCCESS,
        			 'result' => array('data' => $ret));
    	}
         return array('error_code' => Distribution_Conf::NODE_EXIST,
        			 'result' => array('data' => $ret));
    }
}
