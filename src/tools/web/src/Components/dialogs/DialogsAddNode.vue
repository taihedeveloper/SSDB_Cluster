<template>
	<div class="dialog">
		<div class="dialogOverlay"></div>
		<div class="dialogContent">
			<div class="dialogClose" @click="hideDialog"></div>
			<div class="dialogDetail">
				<div class="dialogTitle">新增ssdb节点</div>
				<div class="dialogDiv">
					<div class="dialogSubDiv">
						<div class="subDivTitle requiredItem">主节点地址</div>
						<div class="subDivContent"><input type="text" class="alarm-config-dialog-name" placeholder="" v-model="formData.masterserver"></div>
					</div>
                    <div class="dialogSubDiv">
						<div class="subDivTitle requiredItem">从节点地址</div>
						<div class="subDivContent"><input type="text" class="alarm-config-dialog-name" placeholder="" v-model="formData.slaveserver"></div>
					</div>

					<div class="dialogButtonDiv">
						<a href="javascript:;" class="dialog-blue-button" @click="addNode">保存</a>
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
				masterserver: '',
				slaveserver: ''
			},
			copyformData:{}
		}
	},
	watch: {
		editinitformdata:function(){
			this.formData.masterserver = "";
			this.formData.slaveserver = "";
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
		addNode: function(){
            this.formData.masterserver = vueGetData.trim(this.formData.masterserver);
            this.formData.slaveserver = vueGetData.trim(this.formData.slaveserver);

			let data = {};
            data['masternode'] = this.formData.masterserver;
            data['backnode'] = this.formData.slaveserver;
			let  _self = this;
			vueGetData.getData("addnodes", data, function(jsondata){
				let error_code = jsondata.body.error_code;
		        if(error_code === 22000) {
		        	vueGetData.creatTips("操作成功");
		        	_self.hideDialog();
					this.$store.dispatch('getNodetabledata', {});
		        }else if(error_code === 22001){
		        	vueGetData.creatTips("系统错误请联系管理员查看问题");
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