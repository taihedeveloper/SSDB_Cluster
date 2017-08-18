<?php

class Service_Data_MigrateSlots {
    private $objDaoSample;
    public function __construct(){
        //$this->objDao= new Dao_Nodes();
    }
    
    public function MigrateSlots($arrInput)
    {
        if (NULL == $arrInput['start_slot'] or NULL == $arrInput['end_slot'] or NULL == $arrInput['ip'] or NULL == $arrInput['port'] ){
            return -1;
        }
        for ($i=intval($arrInput['start_slot']); $i <= $arrInput['end_slot']; $i++) { 
            $slots .= $i . ',';
        }

        $bin_path = Distribution_Conf::migrate_bin_path;
        $cmdstr = $bin_path . '  -s ' . $slots . ' -h ' . $arrInput["ip"] .' -p ' . $arrInput["port"] . ' -z ' . Distribution_Conf::zk_host;
    	if(file_exists($bin_path)){
             //var_dump($cmdstr);
            exec($cmdstr, $output, $return_val);
            if ($return_val) {
                return -1;
            }
            // var_dump($output);
            // var_dump($return_val);
        }
        return 0;
    }
}
