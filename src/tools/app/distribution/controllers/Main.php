<?php
/**
 * @name Main_Controller
 * @desc 主控制器,也是默认控制器
 * @author luohongcang@taihe.com
 */
class Controller_Main extends Ap_Controller_Abstract {
	public $actions = array(
		'sample' => 'actions/api/Sample.php',
		'proxy' => 'actions/api/Proxy.php',
		'nodes' => 'actions/api/Nodes.php',
		'slots' => 'actions/api/Slots.php',
		'addnodes' => 'actions/api/AddNodes.php',
		'migrate' => 'actions/api/MigrateSlots.php',
		'meminfo' => 'actions/api/MemInfo.php',
		'stat' => 'actions/api/Stat.php',
		'addproxy' => 'actions/api/AddProxy.php',
	);
}
