<?php
/**
 * @name Dao_Sample
 * @desc sample dao, 可以访问数据库，文件，其它系统等
 * @author luohongcang@taihe.com
 */
class Dao_Stat {
	private $_zk = null;
    public function __construct()
    {
        $this->_zk = new \Zookeeper(Distribution_Conf::zk_host);
    }   
    
    public function getStat()
    {
	$ret = $this->_zk->getChildren('/twemproxy');
	for ($i = 0; $i<count($ret); $i++) {
	    list($ip, $port) = split (':',$ret[$i]);

	    $ret1 = array();

	    for ($j = 0; $j<2; $j++){
		//创建socket
		if(($socket = socket_create(AF_INET,SOCK_STREAM,SOL_TCP)) < 0) {
			continue;
		}

		socket_set_option($socket,SOL_SOCKET,SO_RCVTIMEO,array("sec"=>0, "usec"=>100000 ) );
		socket_set_option($socket,SOL_SOCKET,SO_SNDTIMEO,array("sec"=>0, "usec"=>100000 ) );

		//连接socket
		if(($result = socket_connect($socket, $ip, $port)) < 0){
			continue;
		}

		$in  = "\n";
		$out = '';

		//写数据到socket缓存
		if(!socket_write($socket, $in, strlen($in))) {
			socket_close($socket);
			continue;
		}

		//读取指定长度的数据
		while($out = socket_read($socket, 4096)) {
	        	$ret1 = json_decode($out, true);
		}

		if (!empty($ret1)){
			socket_close($socket);
	    		break;
		}

		socket_close($socket);
	    }

	    $ret1 ["ip"]=$ip;
	    $ret1 ["port"]=$port;

	    $ret3[$i] = $ret1;
	}

	return $ret3;
    }
}
