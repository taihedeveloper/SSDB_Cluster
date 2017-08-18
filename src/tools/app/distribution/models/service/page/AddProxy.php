<?php

class Service_Page_AddProxy {
    private $objServiceDataProxy;
    public function __construct(){
        $this->objServiceDataProxy = new Service_Data_AddProxy();
    }

    public function execute($arrInput){
	$ret = $this->objServiceDataProxy->addProxy($arrInput, $data);
        if (0 != $ret){
            return array('error_code' => 22001,
        			 'result' => array('data' => $data));
	} else {
	    return array('error_code' => 22000,
				'result' => array('data' => "success"));
	}
    }
}
