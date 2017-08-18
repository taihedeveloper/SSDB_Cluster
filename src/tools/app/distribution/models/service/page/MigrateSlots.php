<?php

class Service_Page_MigrateSlots {
    private $objService;
    public function __construct(){
        $this->objService = new Service_Data_MigrateSlots();
    }

    public function execute($arrInput){
        $ret = $this->objService->MigrateSlots($arrInput);
    	if(0 == $ret){
    		return array('error_code' => Distribution_Conf::SUCCESS,
        			 'result' => array('data' => $ret));
    	}
         return array('error_code' => Distribution_Conf::FAILED,
        			 'result' => array('data' => $ret));
    }
}
