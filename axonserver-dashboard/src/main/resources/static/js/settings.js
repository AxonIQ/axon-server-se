/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

//# sourceURL=settings.js
globals.pageView = new Vue({
            el: '#settings',
            data: {
                license: {},
                status: {},
                node: {},
                nodes: []
            }, mounted() {
        this.reloadStatus();
        axios.get("v1/public/license").then(response => { this.license = response.data });
        axios.get("v1/public/me").then(response => { this.node = response.data });
        axios.get("v1/public").then(response => { this.nodes = response.data });
    }, methods: {
        reloadStatus: function () {
            axios.get("v1/public/status").then(response => { this.status = response.data });
        },
        resetEvents() {
            if(confirm("Are you sure you want to delete all event and snapshot data?")){
                axios.delete("v1/devmode/purge-events").then(response => {this.reloadStatus()});
            }

        }
    }
        });
