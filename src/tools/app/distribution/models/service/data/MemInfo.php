<?php

class Service_Data_MemInfo {
    private $objDaoSlots;
    public function __construct(){
        $this->objDao= new Dao_MemInfo();
    }
    
    public function memInfo()
    {
        return $this->objDao->getMemInfo();
    }
}
