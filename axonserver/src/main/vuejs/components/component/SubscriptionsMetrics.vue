<!--
  - Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
  - under one or more contributor license agreements.
  -
  -  Licensed under the AxonIQ Open Source License Agreement v1.0;
  -  you may not use this file except in compliance with the license.
  -
  -->

<template>

    <div class="column-wrapper">
        <div class="column status">
            <section>
                <h2>Subscription Queries</h2>
                <ul>
                    <li>
                        <span>#Total Subscription Queries</span>
                        <span class="number">{{metrics.totalSubscriptions}}</span>
                    </li>

                    <li>
                        <span>#Active Subscription Queries</span>
                        <span class="number">{{metrics.activeSubscriptions}}</span>
                    </li>
                    <li>
                        <span>#Updates to Subscription Queries</span>
                        <span class="number">{{metrics.updates}}</span>
                    </li>
                </ul>
            </section>
        </div>
    </div>
</template>


<script>
    module.exports = {
        name: 'component-subscription-metrics',
        props:['component', 'context'],
        data(){
            return {
                metrics:{},
                webSocketInfo: globals.webSocketInfo
            }
        }, mounted() {
            this.loadComponentSubscriptionsMetrics();
            let me = this;
            this.webSocketInfo.subscribe('/topic/cluster',
                                         me.loadComponentSubscriptionsMetrics,
                                         function (sub) {
                                           me.subscription = sub;
                                         });
        }, beforeDestroy() {
            if( this.subscription) this.subscription.unsubscribe();
        }, methods: {
            loadComponentSubscriptionsMetrics(){
                axios.get("v1/components/"+encodeURI(this.component)+"/subscription-query-metric?context=" + this.context).then(response => {
                    this.metrics = response.data;
                });
            }
        }
    }
</script>

<style scoped>
    div.status {
        width: 650px;
        display: block;
    }
</style>