<?php

class Service_Data_Slots {
    private $objDaoSlots;
    public function __construct(){
        $this->objDao= new Dao_Slots();
    }
    
    public function slots()
    {
        return $this->objDao->getSlots();
    }
}
