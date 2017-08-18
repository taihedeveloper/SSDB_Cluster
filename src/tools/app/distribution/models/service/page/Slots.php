<?php

class Service_Page_Slots {
    private $objServiceDataNodes;
    public function __construct(){
        $this->objServiceDataNodes = new Service_Data_Slots();
    }

    public function execute($arrInput){
        return array('error_code' => Distribution_Conf::SUCCESS,
        			 'result' => array('data' => $this->objServiceDataNodes->slots()));
    }
}
