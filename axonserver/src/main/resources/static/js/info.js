/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

//# sourceURL=info.js
globals.pageView = new Vue(
        {
            el: '#info',
            data: {
                licences: ""
            }, mounted() {
                axios.get("v1/info/third-party").then(response => {
                        this.thirdParty(response.data);
                });
            }, beforeDestroy() {

            }, methods: {
                thirdParty(m) {
                    this.licences = m;
                }
            }
        });