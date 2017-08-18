<template>
	<header class="header header-noborder">
	    <div class="header-container clearfix">
	    	<router-link to="/index" class="logo"></router-link>
	        <div class="user-info mr">
				<div class="username mr-s" style="position: relative;">
					<span>{{username}}</span>
					<ul class="sub-nav t_right" style="right: 0;">
						<li><a href="http://192.168.217.41:8072/logout.php">安全退出</a></li>
					</ul>
				</div>
	        </div>

	        <ul class="nav-list">
	            <li id="index-li" class="">
	            	<router-link to="/index">首页</router-link>
	            </li>
	            <li id="proxy-li" class="nav-ul-selected">
	            	<router-link to="/proxyinfo">代理信息</router-link>
	            </li>
	            <li id="slot-li" class="">
					<router-link to="/slotinfo">数据槽位信息</router-link>
	            </li>
	            <li id="nodes-li" class="nav-ul-selected">
	            	<router-link to="/nodesinfo">节点信息</router-link>
	            </li>
	        </ul>
	    </div>
	</header>
</template>

<script>
import vueGetData from "../Js/vueGetData.js"
	export default {
	name: 'Header',
		data () {
			return {
			msg: 'header',
			username:""
			}
		},
		created(){
			this.username = vueGetData.username();
		}
	}
</script>

<style>
.header {
	height: 68px;
	background: #218868;
	width: 100%;
	z-index: 200;
	position: relative;
	opacity: .95
}

.header.header-noborder {
	box-shadow: 0 0 0 #e5e5e5;
	border-bottom: 0
}

.header .logo {
	display: block;
	width: 350px;
	height: 68px;
	float: left;
	background-image: url(/dist/Image/logo.png);
	background-repeat: no-repeat;
	background-size: contain;
	background-position-y: 10px
}

.header-container {
	width: 97%;
	margin: 0 auto
}

.user-info {
	font-size: 13px;
	position: relative;
	float: right;
	min-width: 100px
}

.user-info .username {
	display: inline-block;
	vertical-align: middle
}

.user-info .username>span {
	width: 100px;
	white-space: nowrap;
	overflow: hidden;
	text-overflow: ellipsis;
	display: inline-block;
	line-height: 68px;
	color: #ccc;
	cursor: pointer;
	vertical-align: middle;
	padding-right: 20px;
	background: url(/dist/Image/down_triangle.png) no-repeat;
	background-position: right center;
	font-size: 14px;
}

.user-info .username:hover>span {
	background: url(/dist/Image/up_triangle.png) no-repeat;
	background-position: right center
}

.user-info a {
	vertical-align: middle;
	color: #ccc
}

.nav-list {
	float: right;
	display: inline-block;
	margin-right: 40px
}

.nav-list>li {
	float: left;
	line-height: 68px;
	width: 100px;
	text-align: center
}

.nav-list>li:hover {
	background: rgba(0, 0, 0, .05)
}

.nav-list li a {
	color: #fff;
	display: block;
	font-size: 15px
}
.nav-list li a.router-link-active{
	background: rgba(0, 0, 0, .1)
}

.nav-list>span {
	float: left;
	line-height: 65px;
	width: 5px;
	text-align: center;
	color:#FFF;
	font-size: 18px;
	font-weight: bold;
}

.sub-nav {
	position: absolute;
	z-index: 10;
	padding: 10px 0;
	border: 1px solid #ccc;
	display: none;
	border-top: 0;
	box-sizing: border-box;
	min-width: 100px;
	background: #fff
}

.sub-nav li>a {
	display: block;
	line-height: 30px;
	text-align: left;
	padding: 0 20px;
	color: #333;
	font-size: 13px
}

.sub-nav li>a:hover {
	background: #ecf3fe;
	color: #4085f4
}

.username:hover .sub-nav,
.nav-list>li:hover .sub-nav {
	display: block
}
</style>