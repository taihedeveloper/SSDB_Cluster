<template>
  <div id="app" class="cf">
    <Headers></Headers>
    <router-view></router-view>
  </div>
</template>

<script>
import {mapGetters,mapActions} from 'vuex'
import Headers from './Headers.vue'

export default {
  name: 'app',
  data () {
    return {
      msg: 'Welcome to My Vue.js App'
    }
  },
  methods:mapActions([
    "increment"
  ]),
  components:{
    Headers,
  },
  computed:mapGetters([
    'count',
    'inputvalue'
  ])
}
</script>
<style lang="less">
@import "../Css/common";
@import "../Css/dialog.less";
@import "../Js/plugins/kalendae/css/kalendae.css";
#app{
  width: 100%;
}
</style>