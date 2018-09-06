<template>
    <div class="results singleHeader">
        <table>
            <thead>
            <tr>
                <th><div>Instance Name</div></th>
                <th><div>AxonHub Server</div></th>
            </tr>
            </thead>
            <tbody class="selectable">
            <tr v-for="instance in instances">
                <td><div>{{instance.name}}</div></td>
                <td><div>{{instance.axonHubServer}}</div></td>
            </tr>
            </tbody>
        </table>
    </div>
</template>


<script>
    module.exports = {
        name: 'component-instances',
        props:['component', 'context'],
        data(){
            return {
                instances:[],
                webSocketInfo: globals.webSocketInfo
            }
        }, mounted() {
            this.loadComponentInstances();
            let me = this;
            this.webSocketInfo.subscribe('/topic/cluster', me.loadComponentInstances, function (sub) {
                me.subscription = sub;
            });
        }, beforeDestroy() {
            if( this.subscription) this.subscription.unsubscribe();
        }, methods: {
            loadComponentInstances(){
                axios.get("v1/components/"+this.component+"/instances?context=" + this.context).then(response => {
                    this.instances = response.data;
                });
            }
        }
    }
</script>

<style scoped>
</style>