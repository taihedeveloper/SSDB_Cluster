<?php
/**
 * @name Service_Page_Sample
 * @desc sample page service, 和action对应，组织页面逻辑，组合调用data service
 * @author luohongcang@taihe.com
 */
class Service_Page_Sample {
    private $objServiceDataSample;
    public function __construct(){
        $this->objServiceDataSample = new Service_Data_Sample();
    }

    public function execute($arrInput){
        return array('error_code' => Distribution_Conf::SUCCESS, 'result' => array('data' => $this->objServiceDataSample->test()));
    }
}
