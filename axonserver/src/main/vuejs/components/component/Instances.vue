<!--
  -  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
  -  under one or more contributor license agreements.
  -
  -  Licensed under the AxonIQ Open Source License Agreement v1.0;
  -  you may not use this file except in compliance with the license.
  -
  -->

<template>
    <div class="results singleHeader">
        <table>
            <thead>
            <tr>
                <th><div>Instance Name</div></th>
                <th><div>AxonServer Node</div></th>
                <th>
                    <div>Tags</div>
                </th>
            </tr>
            </thead>
            <tbody>
            <tr v-for="instance in instances">
              <td>
                <div>{{ instance.name }}</div>
              </td>
              <td>
                <div>{{ instance.axonServerNode }}<span style="color: gray;font-size: 13px;margin-left: 5px;"> &#8592; {{ instance.context }}
</span></div>
              </td>
              <td>
                <ul>
                        <li v-for="tag in Object.keys(instance.tags)">{{tag}}={{instance.tags[tag]}}</li>
                    </ul>
                </td>
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
                axios.get("v1/components/"+encodeURI(this.component)+"/instances?context=" + this.context).then(response => {
                    this.instances = response.data;
                });
            }
        }
    }
</script>

<style scoped>
</style>