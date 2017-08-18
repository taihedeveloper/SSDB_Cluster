<?php

class Service_Data_Nodes {
    private $objDaoSample;
    public function __construct(){
        $this->objDao= new Dao_Nodes();
    }
    
    public function nodes()
    {
        return $this->objDao->getNodes();
    }
}
