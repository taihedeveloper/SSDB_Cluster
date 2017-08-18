<?php

class Action_Stat extends Saf_Api_Base_Action {

    public function execute(){
    	$arrRequest = Saf_SmartMain::getCgi();
        $arrInput = $arrRequest['get'];
		$objServicePageSampleApi = new Service_Page_Stat();
		$arrPageInfo = $objServicePageSampleApi->execute($arrInput);
		$arrOutput = $arrPageInfo;
		if(isset($arrInput['callback']) && !empty($arrInput['callback']))
		{
			echo $arrInput['callback'] . "(" . json_encode($arrOutput) . ")";
		}
		else
		{
			echo json_encode($arrOutput);
		}

        //return $arrPageInfo;
    }
	
    public function __render($arrRes){
    	echo json_encode($arrRes);
    }
	
	public function __value($arrRes){
		echo json_encode($arrRes);
	}
}
