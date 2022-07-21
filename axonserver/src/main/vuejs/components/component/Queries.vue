<!--
  -  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
  -  under one or more contributor license agreements.
  -
  -  Licensed under the AxonIQ Open Source License Agreement v1.0;
  -  you may not use this file except in compliance with the license.
  -
  -->

<template>
    <div id="component-queries" class="results singleHeader">
        <table>
            <thead>
            <tr>
                <th>Query</th>
                <th>Response Types</th>
                <th>#Subscriptions</th>
                <th>#Active Subscriptions </th>
                <th>#Updates</th>
            </tr>
            </thead>
            <tbody>
            <tr v-for="query in queries">
              <td>
                <div>{{ query.name }}</div>
              </td>
              <td>
                <div v-for="response in query.responseTypes">{{ response }}</div>
              </td>
              <td>
                <div>{{ query.metrics.totalSubscriptions }}</div>
              </td>
              <td>
                <div>{{ query.metrics.activeSubscriptions }}</div>
              </td>
              <td>
                <div>{{ query.metrics.updates }}</div>
              </td>
            </tr>
            </tbody>
        </table>
    </div>
</template>


<script>
    module.exports = {
        name: 'component-queries',
        props:['component', 'context'],
        data(){
            return {
              webSocketInfo: globals.webSocketInfo,
                queries: []
            }
        }, mounted() {
            this.loadComponentQueries();
        let me = this;
        this.webSocketInfo.subscribe('/topic/cluster', me.loadComponentQueries, function (sub) {
          me.subscription = sub;
        });
      }, beforeDestroy() {
        if (this.subscription) {
          this.subscription.unsubscribe();
        }
        }, methods: {
            loadComponentQueries(){
                var baseUrl = "v1/components/"+encodeURIComponent(this.component);
                let self = this;
              this.queries = []
                axios.get(baseUrl+"/queries?context=" + this.context).then(response => {
                    var queries = response.data.filter((item, pos, self) => self.findIndex(v => v.name === item.name) === pos);
                    queries.forEach(function (query, index) {
                        axios.get(baseUrl+"/subscription-query-metric/query/"+encodeURIComponent(query.name)+"?context=" + self.context)
                                .then(metric => {
                                    query.metrics = metric.data;
                                    self.queries.push(query);
                                });
                    })
                });
            }
        }
    }
</script>

<style scoped>
</style>