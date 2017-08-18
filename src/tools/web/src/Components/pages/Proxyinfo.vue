<template>
	<div class='proxyclass'>
		<div class="button-area" style="display:block">
			<button class="add-blue-button" @click="newAddProxy"><span></span>添加代理信息</button>
		</div>
		<div class="table-div">
			<table class="manage-table">
				<thead>
					<tr>
						<th width="8%">序号</th><th width="20%">访问地址</th><th width="20%">总连接数</th><th width="17%">当前连接数</th><th width="17%">客户端连接数</th><th width="18%">客户端错误数</th>
					</tr>
				</thead>
				<tbody>
					<tr v-for="(item,index) in nodeinfo">
						<td>{{item.id}}</td>
						<td>{{item.ip}}</td>
						<td>{{item.total_conn}}</td>
						<td>{{item.curr_conn}}</td>
						<td>{{item.client_conn}}</td>
						<td>{{item.client_err}}</td>
					</tr>
				</tbody>
			</table>
		</div>
		<DialogsAddProxy></DialogsAddProxy>
	</div>
</template>

<script>
import vueGetData from "../../Js/vueGetData.js"
import DialogsAddProxy from "../dialogs/DialogsAddProxy.vue"
import {mapGetters,mapActions} from 'vuex'

	export default {
		name: 'Proxyinfo',
		data () {
			return {
				page: 0,
			}
		},
		methods: {
			newAddProxy: function(){
				this.showDialog();
			},
			showDialog: function(){
				let data = {"username":vueGetData.username,"type":4};
				document.getElementsByClassName("dialog")[0].style.display = "block";
			}
		},
		computed:mapGetters({
			nodeinfo: "proxytableList",
		}),
		mounted:function() {
			this.$store.dispatch('getProxytabledata', {});
		},
		components: {
			DialogsAddProxy
		}
	}
</script>

<style lang="less" scoped>
.proxyclass{
	width: 90%;
    margin: 10px auto;
}
.home-title {
	td {
		height: 40px;
		line-height: 40px;
		padding: 0 10px;
		word-break: break-all;
		overflow: hidden;
		white-space: nowrap;
		text-overflow: ellipsis;
		font-weight: bold;
		font-size: 30px;
	}
}
</style>
