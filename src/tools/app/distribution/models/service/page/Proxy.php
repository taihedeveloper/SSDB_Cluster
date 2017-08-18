<?php

class Service_Page_Proxy {
    private $objServiceDataProxy;
    public function __construct(){
        $this->objServiceDataProxy = new Service_Data_Proxy();
    }

    public function execute($arrInput){
        return array('error_code' => Distribution_Conf::SUCCESS,
        			 'result' => array('data' => $this->objServiceDataProxy->proxy()));
    }
}
