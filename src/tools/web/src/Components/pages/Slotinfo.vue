<template>
	<div class="slotinfo">
		<div class="slot_migrate">
			<button class="immigrate-button" @click="immigrate"><span></span>迁移槽位</button>槽位序号从<input type="text" class="alarm-config-dialog-name" placeholder="[0, 16383]" v-model="formInitData.start_index"></input>至<input type="text" class="alarm-config-dialog-name" placeholder="[0, 16383]" v-model="formInitData.end_index"></input>迁移到节点<input type="text" class="alarm-config-dialog-name" placeholder="序号(0,1,2,..)" v-model="formInitData.node_index"></input>
			<table class="slot-table">
			    <tr class='slot-title'>
					<td>
						槽位基本信息
					</td>
				</tr>
				<tbody>
					<tr><td width="20%" style="font-weight: bold">节点序号</td><td width="40%" style="font-weight: bold">主节点地址</td><td width="40%" style="font-weight: bold">节点拥有槽位</td></tr>
					<tr v-for="(item,index) in totalnum">
						<td>{{item.id}}</td>
						<td>{{item.ip}}</td>
						<td>{{item.num}}</td>
					</tr>
				</tbody>
			</table>
		</div>
		<div id="slotmap" class="graph">
		</div>
	</div>
</template>

<script>
import echarts from "../../Js/plugins/echarts.min.js"
import vueGetData from "../../Js/vueGetData.js"

	export default {
		name: 'Slotinfo',
		data(){
			return {
				categories : ['Default', 'Migrating'],
				commondata : [],
				immidata : [],
				commondur : [],
				immidur : [],
				nodenum : [],
				imminodenum : [],
				totalnum : [],
				formInitData:{
					start_index : "",
					end_index : "",
					node_index : ""
				},
				nodeinfo : {}
			}
		},
		mounted: function(){
			var node_info = {}
			var node_check_arr = []
			vueGetData.getData("nodes", {}, function(jsondata){
				if (jsondata.body.error_code === 22000) {
					for (var i = 0; i < jsondata.body.result.data.length; ++i)
					{
						node_info[jsondata.body.result.data[i].num] = jsondata.body.result.data[i].ip + ":" + jsondata.body.result.data[i].port;
					}
					for (i in node_info)
					{
						this.nodeinfo[i] = node_info[i];
					}
				}
			}.bind(this), function(){

	        }.bind(this));

			
			vueGetData.getData("slots", {}, function(jsondata){
	        	if (jsondata.body.error_code === 22000) {
					var init_index = -1;
					var during = 0;
					var immi_index = -1;
					var immi_during = 0;
					var i = 0;
					for (; i < jsondata.body.result.data.length; ++i) {
						if (init_index != jsondata.body.result.data[i].node_index)
						{
							if (during != 0)
							{
								this.commondur.push(during);
								this.nodenum.push(init_index);
							}
							init_index = jsondata.body.result.data[i].node_index;
							during = 0;
							this.commondata.push(i);
						}
						during++;

						if (jsondata.body.result.data[i].migrating != 'false')
						{
							if (immi_index != jsondata.body.result.data[i].node_index)
							{
								if (immi_during != 0)
								{
									this.immidur.push(immi_during);
									this.imminodenum.push(immi_index);
								}
								immi_index = jsondata.body.result.data[i].node_index;
								immi_during = 0;
								this.immidata.push(i);
							}
							immi_during++;
						}
						else
						{
							if (immi_during != 0)
							{
								this.immidur.push(immi_during);
								this.imminodenum.push(immi_index);
							}
							immi_index = -1;
							immi_during = 0;
						}
					}
					if (during != 0) {
						this.commondur.push(during);
						this.nodenum.push(init_index);
					}
					if (immi_during != 0)
					{
						this.immidur.push(immi_during);
						this.imminodenum.push(immi_index);
					}

					var num_temp = {};
					for (var j = 0; j < this.nodenum.length; ++j)
					{
						if (!num_temp[this.nodenum[j]])
						{
							num_temp[this.nodenum[j]] = 0;
						}
						num_temp[this.nodenum[j]] = parseInt(num_temp[this.nodenum[j]]) + parseInt(this.commondur[j]);
					}

					for (var k in num_temp)
					{
						let obj_temp = {}
						obj_temp['id'] = k;
						obj_temp['ip'] = node_info[k];
						obj_temp['num'] = num_temp[k];
						this.totalnum.push(obj_temp);
						node_check_arr.push(node_info[k]);
					}
					for (var l in node_info)
					{
						if (node_check_arr.indexOf(node_info[l]) == -1)
						{
							let obj_temp = {}
							obj_temp['id'] = l;
							obj_temp['ip'] = node_info[l];
							obj_temp['num'] = 0;
							this.totalnum.push(obj_temp);
						}
					}
					this.totalnum.sort(function(a, b){
						return a.id - b.id;
					}) 
					this.drawpic(this.commondata, this.commondur, this.nodenum, this.immidata, this.immidur, this.imminodenum);
	        	}
	        }.bind(this), function(){

	        }.bind(this));
		},

		methods: {
		    renderItem:function(params, api) {
				var categoryIndex = api.value(0);
				var start = api.coord([api.value(1), categoryIndex]);
				var end = api.coord([api.value(2), categoryIndex]);
				var height = api.size([0, 1])[1] * 0.6;

				return {
					type: 'rect',
					shape: echarts.graphic.clipRectByRect({
						x: start[0],
						y: start[1] - height / 2,
						width: end[0] - start[0],
						height: height
					}, {
						x: params.coordSys.x,
						y: params.coordSys.y,
						width: params.coordSys.width,
						height: params.coordSys.height
					}),
					style: api.style()
				};
			},
			drawpic:function(commondata, commondur, nodenum, immidata, immidur, imminodenum) {
				var myChart = echarts.init(document.getElementById('slotmap'));
				var types = ['#7b9ce1', '#bd6d6c', '#75d874', '#e0bc78', '#dc77dc', '#72b362'];
				var data = [];
				echarts.util.each(this.categories, function (category, index) {
					if (category == 'Default'){
						for (var i = 0; i < commondata.length; ++i)
						{
							var typeItem = types[nodenum[i]];
							var commondata_end = parseInt(commondata[i]) + parseInt(commondur[i]) - 1;
							data.push({
								name: "node: " + nodenum[i] + " interval: " + commondata[i] + "-" + commondata_end,
								value: [
									index,
									commondata[i],
									commondata[i] + commondur[i],
									commondur[i]
								],
								itemStyle: {
									normal: {
										color: typeItem
									}
								}
							});
					 	}
					}
					if (category == 'Migrating'){
						for (var i = 0; i < immidata.length; ++i)
						{
							var typeItem = types[i];
							data.push({
								name: typeItem.name,
								value: [
									index,
									immidata[i],
									immidata[i] + immidur[i],
									immidur[i]
								],
								itemStyle: {
									normal: {
										color: typeItem
									}
								}
							});
					 	}
					}
				});

				var option = {
					tooltip: {
						formatter: function (params) {
							return params.marker + params.name + ' length: ' + params.value[3];
						}
					},
					title: {
						text: '槽位分布信息',
						left: 'center'
					},
					xAxis: {
						min: 0,
						max: 16384,
						interval: 1024,
						type: 'value',
						scale: true,
						axisTick: {
							alignWithLabel: true
						},
						axisLabel: {
							formatter: function (val) {
								return val;
							}
						},
						splitLine: {
							show: false
						}
					},
					yAxis: {
						data: this.categories
					},
					series: [{
						type: 'custom',
						renderItem: this.renderItem,
						itemStyle: {
							normal: {
								opacity: 0.8
							}
						},
						encode: {
							x: [1, 2],
							y: 0
						},
						data: data
					}]
				};
				myChart.setOption(option);
			},
			immigrate :function() {
				if (this.formInitData.start_index == "" || this.formInitData.end_index == "" || this.formInitData.node_index == "")
				{
					vueGetData.creatTips("参数不完整，请输入正确参数");
					return;
				}
				let immi_data = {};
				immi_data['start_slot'] = this.formInitData.start_index;
				immi_data['end_slot'] = this.formInitData.end_index;
				let strs = this.nodeinfo[this.formInitData.node_index].split(":");
				let immi_ip = strs[0];
				let immi_port = strs[1];
				immi_data['ip'] = immi_ip;
				immi_data['port'] = immi_port;
				vueGetData.getData("migrate", immi_data, function(jsondata){
					if (jsondata.body.error_code === 22000) {
						vueGetData.creatTips("操作成功");
					}
					if (jsondata.body.error_code === 22001) {
						vueGetData.creatTips("操作失败");
					}
				}.bind(this), function(){

				}.bind(this));
			}
		}
	}
</script>

</script>
<style lang="less">
html {
	background: none;	
}
.slotinfo{
	padding:20px 0px 10px;
	width: 90%;
	margin: 0px auto;

	.graph {
		width: 80%;
		margin: 0px auto;
		height:400px;
		padding:20px 20px 10px;
	}

	.slot_migrate {
		max-height: 350px;
		padding: 10px 10px 10px;
		overflow-y: auto;

		.slot-table {
			width: 100%;
			margin: 20px auto;
			.slot-title {
				td {
					height: 40px;
					line-height: 40px;
					padding: 0 10px;
					word-break: break-all;
					overflow: hidden;
					white-space: nowrap;
					text-overflow: ellipsis;
					font-weight: bold;
					font-size: 20px;
					margin: 10px 10px;
					header {
						font-weight: bold;
						font-size: 20px;
					}
				}
			}
		}
	}
}

</style>