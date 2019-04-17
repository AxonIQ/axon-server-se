/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

//# sourceURL=queries.js
globals.pageView = new Vue(
        {
            el: '#query',
            data: {
                clients: [],
                componentNames: [],
                queries: [],
                metrics: [],
                webSocketInfo: globals.webSocketInfo
            }, mounted() {
                axios.get("v1/public/query-metrics").then(response => {
                    response.data.forEach(m => {
                        this.loadMetric(m);
                    });
                });
                this.connect();
            }, beforeDestroy() {
                if( this.subscription) this.subscription.unsubscribe();
            }, methods: {
                loadMetric(m) {
                    if (this.clients.indexOf(m.clientId) === -1) {
                        this.clients.push(m.clientId);
                        Vue.set(this.componentNames, m.clientId, m.componentName);
                    }
                    let queryName = m.queryDefinition.queryName;
                    if (this.queries.indexOf(queryName) === -1) {
                        this.queries.push(queryName);
                    }

                    if (!this.metrics[m.clientId]) {
                        Vue.set(this.metrics, m.clientId, []);
                    }
                    Vue.set(this.metrics[m.clientId], queryName, m.count);

                },
                connect() {
                    let me = this;
                        me.webSocketInfo.subscribe('/topic/queries', function (metric) {
                            me.loadMetric(JSON.parse(metric.body));
                        }, function(sub) {
                            me.subscription = sub;
                        });
                }
            }
        });