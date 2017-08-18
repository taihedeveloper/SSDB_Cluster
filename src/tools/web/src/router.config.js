import Home from './Components/pages/Home.vue'
import Proxyinfo from './Components/pages/Proxyinfo.vue'
import Slotinfo from './Components/pages/Slotinfo.vue'
import Nodesinfo from './Components/pages/Nodesinfo.vue'

export default{
	routes:[
		{path:'/index',component:Home},
		{path:'/proxyinfo',component:Proxyinfo},
		{path:'/slotinfo',component:Slotinfo},
		{path:'/',component:Home},
		{path:'*',redirect:'/index'},
		{path:'/nodesinfo',component:Nodesinfo},
	]
}