<!--
  - Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
  - under one or more contributor license agreements.
  -
  -  Licensed under the AxonIQ Open Source License Agreement v1.0;
  -  you may not use this file except in compliance with the license.
  -
  -->

<template>
    <div id="component-command-metrics" class="content results">
        <table id="commands" class="table table-striped">
            <thead>
            <tr>
                <th class="commandCol">Commands</th>
                <th v-for="clientId in clients">
                    <div>{{componentNames[clientId]}} <br>
                        <small>{{clientId}}</small>
                    </div>
                </th>
            </tr>
            </thead>
            <tbody id="statistics">
            <tr v-for="commandName in commands">
                <td><div>{{commandName}}</div></td>
                <td v-for="clientId in clients">{{metrics[clientId][commandName] || 0}}</td>
            </tr>
            </tbody>
        </table>
    </div>
</template>

<script>
import {v4 as uuidv4} from 'uuid';

export default {
        name: 'component-command-metrics',
        props: [],
        data: function() {
            return {
                clients: [],
                componentNames: [],
                commands: [],
                metrics: [],
                webSocketInfo: globals.webSocketInfo
            }
        }, mounted: function() {
            this.loadMetrics();
        this.connect(this);
        }, beforeDestroy: function() {
            if( this.subscription) this.subscription.unsubscribe();
        }, methods: {
        loadMetrics() {
          axios.get("v1/public/command-metrics").then(response => {
            this.metrics = [];
            response.data.forEach(m => {
              this.loadMetric(m);
            });
          });
        },
        loadMetric(m) {
          if (this.clients.indexOf(m.clientId) === -1) {
            this.clients.push(m.clientId);
            Vue.set(this.componentNames, m.clientId, m.componentName + "@" + m.context);
          }
          if (this.commands.indexOf(m.command) === -1) {
            this.commands.push(m.command);
          }

          if (!this.metrics[m.clientId]) {
                    Vue.set(this.metrics, m.clientId, []);
                }
                Vue.set(this.metrics[m.clientId], m.command, m.count);

            },
            connect(me) {
              me.webSocketInfo.subscribe('/topic/commands/' + uuidv4(), function (metric) {
                me.loadMetric(JSON.parse(metric.body));
              }, function (subscription) {
                me.subscription = subscription;
              }, me.loadMetrics);
            }
        }
    };
</script>