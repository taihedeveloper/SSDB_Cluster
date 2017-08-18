<?php

class Service_Data_Proxy {
    private $objDaoSample;
    public function __construct(){
        $this->objDao= new Dao_Proxy();
    }
    public function send_post($url, $post_data) {
 
	    $postdata = http_build_query($post_data);
	    $options = array(
	        'http' => array(
	            'method' => 'POST',
	            'header' => 'Content-type:application/x-www-form-urlencoded',
	            'content' => $postdata,
	            'timeout' => 15 * 60 // 超时时间（单位:s）
	        )
	    );
	    $context = stream_context_create($options);
	    $result = file_get_contents($url, false, $context);
	 
	    return $result;
	}
    public function proxy()
    {
        // return $this->objDao->getProxy();
        return json_decode(file_get_contents(Distribution_Conf::twemproxy_stat_url ));
    }
}
