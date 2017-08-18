import Vue from 'vue'
import Vuex from 'vuex'

import manage from './modules/manage.js'

const debug = process.env.NODE_ENV !== 'production'
Vue.use(Vuex)
Vue.config.debug = debug


//导出store对象
export default new Vuex.Store({
    //组合各个模块
    modules:{
        manage
    },
    strict: debug

})
