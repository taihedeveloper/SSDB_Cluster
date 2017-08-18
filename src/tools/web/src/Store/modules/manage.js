import * as  types from '../mutation-types.js'
import vueGetData from "../../Js/vueGetData.js"

 //定义变量
const state = {
    proxytablelist : [],
    nodetablelist : [],
}

//事件处理：异步请求、判断、流程控制
const actions = {
    getProxytabledata:function({commit},paramsJson){
        let proxyinfo = [];
        vueGetData.getData("stat", {}, function(jsondata){
            if (jsondata.body.error_code === 22000) {
                for (var i = 0; i < jsondata.body.result.data.length; ++i) {
                    let temp = {}
                    temp['id'] = i;
                    temp['ip'] = jsondata.body.result.data[i].ip + ":" + jsondata.body.result.data[i].port;
                    temp['total_conn'] = jsondata.body.result.data[i].total_connections;
                    temp['curr_conn'] = jsondata.body.result.data[i].curr_connections;
                    if (jsondata.body.result.data[i].alpha)
                    {
                        temp['client_err'] = jsondata.body.result.data[i].alpha.client_err;
                        temp['client_conn'] = jsondata.body.result.data[i].alpha.client_connections;
                    }
                    proxyinfo.push(temp);
                }
                console.log(proxyinfo)
                commit(types.PROXYTABLELIST, {proxyinfo});
            }
        }.bind(this), function(){

        }.bind(this));
    },

    getNodetabledata:function({commit},paramsJson){
        var data_temp = {};
        vueGetData.getData("nodes", {}, function(jsondata){
            if (jsondata.body.error_code === 22000){
                for (var i = 0; i < jsondata.body.result.data.length; ++i)
                {
                    var host_ip = jsondata.body.result.data[i].ip;
                    let temp = {};
                    temp['id'] = jsondata.body.result.data[i].num;
                    temp['ip'] = jsondata.body.result.data[i].ip + ":" + jsondata.body.result.data[i].port;
                    temp['slave_ip'] = jsondata.body.result.data[i].slave_ip + ":" + jsondata.body.result.data[i].slave_port;
                    data_temp[temp['ip']] = temp;
                }
                let nodeinfo = [];
                vueGetData.getData("meminfo", {}, function(jsondata){
                    if (jsondata.body.error_code === 22000){
                        for (var i = 0; i < jsondata.body.result.data.length; ++i)
                        {
                            var host_ip = jsondata.body.result.data[i].ip;
                            var port = jsondata.body.result.data[i].port;
                            var ip_key = host_ip + ":" + port;
                            let temp = data_temp[ip_key];
                            temp['meminfo'] = jsondata.body.result.data[i].mem_info;
                            temp['slave_mem_info'] = jsondata.body.result.data[i].slave_mem_info;
                            nodeinfo.push(temp);
                        }
                        nodeinfo.sort(function(a, b){
                            return a.id - b.id;
                        });
                        commit(types.NODETABLELIST, {nodeinfo});
                    }
                }.bind(this), function(){

                }.bind(this));
            }
        }.bind(this),function(){

        }.bind(this));
        
        
    }
}

//处理状态、数据的变化
const mutations = {
    [types.PROXYTABLELIST](state, params){
        state.proxytablelist = params.proxyinfo;
    },
    [types.NODETABLELIST](state, params){
        state.nodetablelist = params.nodeinfo;
    }
}

//导出数据
const getters = {
    proxytableList(state){
        return state.proxytablelist;
    },
    nodetableList(state){
        return state.nodetablelist;
    }
}

export default{
    state,
    actions,
    mutations,
    getters
}