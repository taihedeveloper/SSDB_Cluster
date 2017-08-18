<template>
	<div class="dialog">
		<div class="dialogOverlay"></div>
		<div class="dialogContent">
			<div class="dialogClose" @click="hideDialog"></div>
			<div class="dialogDetail">
				<div class="dialogTitle">新增代理信息</div>
				<div class="dialogDiv">
					<div class="dialogSubDiv">
						<div class="subDivTitle requiredItem">代理节点ip</div>
						<div class="subDivContent"><input type="text" class="alarm-config-dialog-name" placeholder="" v-model="formData.proxyip"></div>
					</div>
                    <div class="dialogSubDiv">
						<div class="subDivTitle requiredItem">代理节点端口</div>
						<div class="subDivContent"><input type="text" class="alarm-config-dialog-name" placeholder="" v-model="formData.proxyport"></div>
					</div>

					<div class="dialogButtonDiv">
						<a href="javascript:;" class="dialog-blue-button" @click="addProxy">保存</a>
						<a href="javascript:;" class="dialog-gray-button" @click="hideDialog">取消</a>
					</div>
				</div>
			</div>
		</div>
	</div>
</template>
<script>
import vueGetData from "../../Js/vueGetData.js"

export default {
	name: "Dialog",
	data () {
		return {
			formData: {
				proxyip: '',
				proxyport: ''
			},
			copyformData:{}
		}
	},
	watch: {
		editinitformdata:function(){
			this.formData.proxyip = "";
			this.formData.proxyport = "";
		}
	},
	methods: {
		hideDialog: function(){
			//重置初始化数据
			for(let key in this.copyformData){
				this.formData[key] = this.copyformData[key]
			}
			document.getElementsByClassName("dialog")[0].style.display = "none";
		},
		addProxy: function(){
            this.formData.proxyip = vueGetData.trim(this.formData.proxyip);
            this.formData.proxyport = vueGetData.trim(this.formData.proxyport);

			let data = {};
            data['ip'] = this.formData.proxyip;
            data['port'] = this.formData.proxyport;
			let  _self = this;
			vueGetData.getData("addproxy", data, function(jsondata){
				let error_code = jsondata.body.error_code;
		        if(error_code === 22000) {
		        	vueGetData.creatTips("操作成功");
		        	_self.hideDialog();
                    _self.$store.dispatch('getProxytabledata', {});
		        }else if(error_code === 22001){
					let return_data = jsondata.body.result.data;
					if (return_data == "the ip and port can not be accessed")
					{
						vueGetData.creatTips("IP与端口不可连接");
					}
		        	else if(return_data.indexOf("exist"))
					{
						vueGetData.creatTips("twemproxy配置已存在");
					}
					else
					{
						vueGetData.creatTips("系统错误请联系管理员查看");
					}
		        	_self.hideDialog();
		        }else if(error_code === 22005){
		        	vueGetData.creatTips("参数错误请重新填写");
		        }else if(error_code === 22008){
		        	vueGetData.creatTips("操作非法");
		        	_self.hideDialog();
		        }else if(error_code === 22009){
		        	vueGetData.creatTips(jsondata.body.result.data);
		        	_self.hideDialog();
		        }else if(error_code === 22452){
		        	vueGetData.creatTips("用户未登录");
		        	_self.hideDialog();
		        }
                
	        },function(err){
		        console.log(err);
	        })

		}
	},
	created: function(){
		for(let key in this.formData){
			this.copyformData[key] = this.formData[key]
		}

	},
	mounted: function(){

	}

}
</script>
<style lang="less">
.dialogSubDiv {
	div{

		.info-label {
			width: 100%;
			margin: 0;
			font-size: 13px;
			line-height: 40px;

			span.lebel-sm{
				padding: 3px 6px;

				label{
					width: 30px !important;
				}
			}
		}
	}
}
</style>