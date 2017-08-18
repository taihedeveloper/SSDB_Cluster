<?php
/**
 * @name Service_Data_Sample
 * @desc sample data service, 按主题组织数据, 提供细粒度数据接口
 * @author luohongcang@taihe.com 
 */
class Service_Data_Sample {
    private $objDaoSample;
    public function __construct(){
        $this->objDaoSample = new Dao_Sample();
    }
    
    public function test()
    {
        return $this->objDaoSample->getTest();
    }
}
