<?php
/**
 * @name sampleScript
 * @desc 示例脚本
 * @author luohongcang@taihe.com
 */
Bd_Init::init();

//主体功能逻辑写在这里
echo 'Hello, sample script running...';

//如果利用noah ct任务系统运行脚本，需要显示退出，设置退出码为0，否则监控系统会报警
exit(0);
