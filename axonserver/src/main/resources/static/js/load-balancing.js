/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

//# sourceURL=load-balancing.js
globals.pageView = new Vue(
        {
            el: '#load-balance',
            data: {
                strategy: {},
                strategies: [],
                factories: [],
                feedback: "",
                webSocketInfo: globals.webSocketInfo,
                admin: globals.admin
            }, computed: {}, mounted() {
                axios.get("v1/processors/loadbalance/strategies/factories").then(
                        response => this.factories = response.data);
                this.loadStrategies();
                this.connect();
            }, beforeDestroy() {
                if (this.subscription) {
                    this.subscription.unsubscribe();
                }
            }, methods: {
                loadStrategies() {
                    axios.get("v1/processors/loadbalance/strategies").then(
                            response => this.strategies = response.data);
                }, saveStrategy(strategy) {
                    axios.post('v1/processors/loadbalance/strategies', strategy).then(response => {
                        this.feedback = strategy.name + " saved.";
                        this.strategy = {};
                        this.loadStrategies();
                    });
                }, changeFactoryBean(strategy) {
                    axios.patch('v1/processors/loadbalance/strategies/' + encodeURIComponent(strategy.name) +
                                '/factoryBean/' + strategy.factoryBean).then(response => {
                        this.feedback = strategy.name + " factoryBean update to " + strategy.factoryBean;
                        this.strategy = {};
                        this.loadStrategies();
                    });
                }, changeLabel(strategy) {
                    axios.patch('v1/processors/loadbalance/strategies/' + encodeURIComponent(strategy.name) +
                            '/label/' + strategy.label).then(response => {
                        this.feedback = strategy.name + " label update to " + strategy.label;
                        this.strategy = {};
                        this.loadStrategies();
                    });
                }, deleteStrategy(strategy) {
                    if (confirm("Delete strategy: " + strategy.name)) {
                        axios.delete('v1/processors/loadbalance/strategies/' + encodeURIComponent(strategy.name))
                                .then(response => {
                                    this.feedback = strategy.name + " deleted.";
                                    this.strategy = {};
                                    this.loadStrategies();
                                }).catch(error => {
                                    this.feedback = error.response.data.message;
                        });
                    }
                }, disableSaveButton() {
                    return !this.strategy.name || !this.strategy.label || !this.strategy.factoryBean;
                }, connect() {
                    let me = this;
                    me.webSocketInfo.subscribe('/topic/application', function () {
                        me.loadStrategies();
                    }, function (sub) {
                        me.subscription = sub;
                    });
                }
            }
        });
