<?php
include_once('phpcas/libraries/CAS.php');
phpCAS::client(CAS_VERSION_2_0, 'cas.taihenw.com', 80, 'cas');
phpCAS::setNoCasServerValidation();
phpCAS::handleLogoutRequests();
if (phpCAS::checkAuthentication())
{
    $username = phpCAS::getUser();
}
else
{
    phpCAS::forceAuthentication();
}
setcookie("personalid", $username);

//获取完整的url
$url = 'http://'.$_SERVER['HTTP_HOST'].$_SERVER['REQUEST_URI'];
$user_select = "fe/template/user_select/index.html";

if(strpos($url,$user_select) !== false){
    echo file_get_contents($user_select);
}else{
    echo file_get_contents('index.html');
}
?>
