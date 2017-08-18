<template>
	<div class="home">
		<div class="graphbox clearfix" style="">
			<table class="home-table">
				<tr class = "home-title">
					<td>
						Overview
					</td>
				</tr>
				<thead></thead>
				<tbody>
					<tr><td width="20%">twemproxy QPS</td><td width="80%">{{qps}}</td></tr>
					<tr><td width="20%">total connections</td><td width="80%">{{total_conn}}</td></tr>
					<tr><td width="20%">current connections</td><td width="80%">{{curr_conn}}</td></tr>
				</tbody>
			</table>
			<div id="down" class="graph"></div>
		</div>
	</div>
</template>

<script>
import echarts from "../../Js/plugins/echarts.js"
import vueGetData from "../../Js/vueGetData.js"
	export default{
		name: 'home',
		data () {
			return {
				graphdata : [],
				qps : "",
				total_conn : "",
				curr_conn : ""
			}
		},
		mounted: function(){
			clearInterval(timers);
			let data={};
			var _self = this;
			data = {"username":vueGetData.username};
			var myChart = echarts.init(document.getElementById('down'));
			var value = Math.random() * 1000;

			function randomData() {
				var now = new Date();
				value = value + Math.random() * 21 - 10;
				return {
					name: now.toString(),
					value: [
						now,
						Math.round(value)
					]
				}
			}

            this.graphdata.push(randomData);
			var option = {
				title: {
					text: 'QPS时序图'
				},
				tooltip: {
					trigger: 'axis',
					formatter: function (params) {
						if (params.length > 0 && params[0].name)
						{
							params = params[0];
							var date = new Date(params.name);
							var month_num = date.getMonth() + 1;
							if (month_num < 10)
							{
								month_num = '0' + month_num;
							}
							var day_num = date.getDate()
							if (day_num < 10)
							{
								day_num = '0' + day_num;
							}
							var hour_num = date.getHours();
							if (hour_num < 10)
							{
								hour_num = '0' + hour_num;
							}
							var minute_num = date.getMinutes();
							if (minute_num < 10)
							{
								minute_num = '0' + minute_num;
							}
							var second_num = date.getSeconds();
							if (second_num < 10)
							{
								second_num = '0' + second_num;
							}
							return day_num + '/' + month_num + '/' + date.getFullYear() + ' ' + hour_num + ':' + minute_num + ":" + second_num + '<br/>' + "OP/s: " + params.value[1];
						}
					},
					axisPointer: {
						animation: false
					}
				},
				xAxis: {
					type: 'time',
					axisLabel: {
						formatter: function (value, index) {
							var date = new Date(value);
							var hour_num = date.getHours();
							if (hour_num < 10)
							{
								hour_num = '0' + hour_num;
							}
							var minute_num = date.getMinutes();
							if (minute_num < 10)
							{
								minute_num = '0' + minute_num;
							}
							var second_num = date.getSeconds();
							if (second_num < 10)
							{
								second_num = '0' + second_num;
							}
							var texts = [hour_num, minute_num, second_num];
							return texts.join(':');
						}
					},
					splitLine: {
						show: false
					}
				},
				yAxis: {
					show: false
				},
				series: [{
					name: 'QPS时序图',
					type: 'line',
					smooth: true,
					symbolSize: 10,
					data: this.graphdata
				}]
			};
			myChart.setOption(option);

			let data_temp = [];
			vueGetData.getData("nodes", {}, function(jsondata){
				if (jsondata.body.error_code === 22000){
					for (var i = 0; i < jsondata.body.result.data.length; ++i)
					{
						data_temp.push(jsondata.body.result.data[i].ip);
					}
				}
			}.bind(this),function(){

			}.bind(this));

			var timers = setInterval(function () {
				vueGetData.getData("stat", {}, function(jsondata){
					let temp_qps = 0;
					let temp_total_conn = 0;
					let temp_curr_conn = 0;
					if (jsondata.body.error_code === 22000) {
						for (var i = 0; i < jsondata.body.result.data.length; ++i)
						{
							var alpha_info = jsondata.body.result.data[i].alpha;
							for (var j = 0; j < data_temp.length; ++j)
							{
								if (alpha_info && alpha_info[data_temp[j]])
								{
									temp_qps = temp_qps + alpha_info[data_temp[j]].requests;
								}
							}
							if (jsondata.body.result.data[i].total_connections)
							{
								temp_total_conn = temp_total_conn + jsondata.body.result.data[i].total_connections;
							}
							if (jsondata.body.result.data[i].curr_connections)
							{
								temp_curr_conn = temp_curr_conn + jsondata.body.result.data[i].curr_connections;
							}
						}
						_self.qps = temp_qps;
						_self.total_conn = temp_total_conn;
						_self.curr_conn = temp_curr_conn;
						var now = new Date();
						let output = {}

						output['name'] = now.toString();
						output['value'] = [];
						output['value'].push(now);
						output['value'].push(Math.round(_self.qps));

						if (_self.graphdata.length > 30)
						{
							_self.graphdata.shift();
						}
						_self.graphdata.push(output);

						myChart.setOption(option);
					}
				}.bind(this),function(){

				}.bind(this));
			}, 5000);
		}
	}
</script>

<style lang="less">
.home{
	padding:20px 20px 10px;
	width: 90%;
	margin: 10px auto;

    .home-table {
		margin: 0px auto;
		padding: 10px 10px 10px;
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
				font-size: 20px;
			}
		}
	}

	.graph {
		margin: 0px auto;
		height:400px;
		padding:20px 20px 10px;
	}
}
</style>