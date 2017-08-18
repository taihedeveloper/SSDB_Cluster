<?php
/**
 * @name Service_Data_Sample
 * @desc sample data service, 按主题组织数据, 提供细粒度数据接口
 * @author luohongcang@taihe.com 
 */
class Service_Data_Stat {
    private $objDao;
    public function __construct(){
        $this->objDao = new Dao_Stat();
    }
    
    public function stat()
    {
        return $this->objDao->getStat();
    }
}
