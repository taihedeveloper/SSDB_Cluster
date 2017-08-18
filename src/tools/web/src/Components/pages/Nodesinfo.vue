<template>
	<div class="nodeinfo">
		<div class="button-area" style="display:block">
			<button class="add-blue-button" @click="newAddNode"><span></span>新增存储节点</button>
		</div>
		<div class="table-div">
			<table class="manage-table">
				<thead>
					<tr>
						<th width="10%">序号</th><th width="20%">主节点地址</th><th width="20%">从节点地址</th><th width="25%">主节点内存</th><th width="25%">从节点内存</th>
					</tr>
				</thead>
				<tbody>
					<tr v-for="(item,index) in nodeinfo">
						<td>{{item.id}}</td>
						<td>{{item.ip}}</td>
						<td>{{item.slave_ip}}</td>
						<td>{{item.meminfo}}</td>
						<td>{{item.slave_mem_info}}</td>
					</tr>
				</tbody>
			</table>
		</div>
		<DialogsAddNode></DialogsAddNode>
	</div>
</template>

<script>
import vueGetData from "../../Js/vueGetData.js"
import DialogsAddNode from "../dialogs/DialogsAddNode.vue"
import {mapGetters,mapActions} from 'vuex'

	export default {
		name: 'Nodesinfo',
		data () {
			return {
				page : 0
			}
		},
		computed:mapGetters({
			nodeinfo: "nodetableList",
		}),
		methods: {
			getNodeinfo:function(){
				this.$store.dispatch('getNodetabledata', {});
			},
			newAddNode: function(){
				this.showDialog();
			},
			showDialog: function(){
				let data = {"username":vueGetData.username,"type":4};
				document.getElementsByClassName("dialog")[0].style.display = "block";
			}
		},
		mounted: function(){
			this.getNodeinfo();
		},
		components: {
			DialogsAddNode
		}
	}
</script>

<style lang="less" scoped>
.nodeinfo{
	width: 90%;
    margin: 10px auto;
}
</style>