<!--
  - Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
  - under one or more contributor license agreements.
  -
  -  Licensed under the AxonIQ Open Source License Agreement v1.0;
  -  you may not use this file except in compliance with the license.
  -
  -->
<template>
  <div class="content results">
    <table id="queries" class="table table-striped">
      <thead>
      <tr>
        <th class="queryCol">Queries</th>
        <th v-for="clientId in clients">
          <div>{{ componentNames[clientId] }} <br>
            <small>{{ clientId }}</small>
          </div>
        </th>
      </tr>
      </thead>
      <tbody id="statistics">
      <tr v-for="queryName in queries">
        <td>
          <div>{{ queryName }}</div>
        </td>
        <td v-for="clientId in clients">{{ metrics[clientId][queryName] || 0 }}</td>
      </tr>
      </tbody>
    </table>
  </div>
</template>
<script>
import {v4 as uuidv4} from 'uuid';

export default {
  name: 'component-command-metrics',
  data: function () {
    return {
      clients: [],
      componentNames: [],
      queries: [],
      metrics: [],
      webSocketInfo: globals.webSocketInfo
    }
  },
  mounted() {
    this.loadMetrics();
    this.connect();
  }
  ,
  beforeDestroy() {
    if (this.subscription) {
      this.subscription.unsubscribe();
    }
  }
  ,
  methods: {
    loadMetrics() {
      axios.get("v1/public/query-metrics").then(response => {
        this.metrics = [];
        response.data.forEach(m => {
          this.loadMetric(m);
        });
      });
    }
    ,
    loadMetric(m) {
      if (this.clients.indexOf(m.clientId) === -1) {
        this.clients.push(m.clientId);
        Vue.set(this.componentNames, m.clientId, m.componentName + "@" + m.context);
      }
      let queryName = m.queryDefinition.queryName;
      if (this.queries.indexOf(queryName) === -1) {
        this.queries.push(queryName);
      }

      if (!this.metrics[m.clientId]) {
        Vue.set(this.metrics, m.clientId, []);
      }
      Vue.set(this.metrics[m.clientId], queryName, m.count);

    }
    ,
    connect() {
      let me = this;
      me.webSocketInfo.subscribe('/topic/queries/' + uuidv4(), function (metric) {
        me.loadMetric(JSON.parse(metric.body));
      }, function (sub) {
        me.subscription = sub;
      }, me.loadMetrics);
    }
  }
}
</script>