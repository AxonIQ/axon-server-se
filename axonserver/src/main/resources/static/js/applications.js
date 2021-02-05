/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

//# sourceURL=application.js
globals.pageView = new Vue(
        {
            el: '#application',
            data: {
                application: {
                    workingRoles: [], metaData: {},
                    metaDataLabel: "",
                    metaDataValue: "",
                },
                applications: [],
                contexts: [],
                appRoles: [],
                newRole: {},
                feedback: "",
                webSocketInfo: globals.webSocketInfo,
                admin: globals.admin
            }, computed: {}, mounted() {
                axios.get("v1/roles/application").then(response => this.appRoles = response.data);
                axios.get("v1/public/context").then(response => {
                    this.contexts = response.data;
                    this.contexts.push({context: "*"});
                });
                this.loadApplications();
                this.connect();
            }, beforeDestroy() {
                if( this.subscription) this.subscription.unsubscribe();
            }, methods: {
                    loadApplications() {
                        axios.get("v1/public/applications").then(response => {
                            this.applications = response.data;

                            for (let a = 0; a < this.applications.length; a++) {
                                let app = this.applications[a];
                                app.workingRoles = [];
                                for (let c = 0; c < app.roles.length; c++) {
                                    let ctx = app.roles[c];
                                    for (let r = 0; r < ctx.roles.length; r++) {
                                        app.workingRoles.push({"context": ctx.context, "role": ctx.roles[r]});
                                    }
                                }
                            }

                            this.application.metaData = {};
                            this.application.metaDataLabel = "";
                            this.application.metaDataValue = "";
                        });
                    },

                    saveApp(application) {
                        if (this.newRole.context && this.newRole.role) {
                            this.addNewRole(this.newRole);
                        }
                        if (application.metaDataLabel && application.metaDataValue) {
                            this.addMetaData();
                        }

                        this.application.roles = [];
                        for (let r = 0; r < this.application.workingRoles.length; r++) {
                            let context = this.getContextRoles(this.application.workingRoles[r].context);
                            context.roles.push(this.application.workingRoles[r].role);
                        }

                        this.application.workingRoles = null
                        axios.post('v1/applications', this.application).then(response => {
                            this.feedback = application.name + ": Application Token: " + response.data;
                            this.application = {
                                roles: [],
                                workingRoles: [],
                                metaData: {},
                                metaDataLabel: "",
                                metaDataValue: "",
                                name: "",
                                description: ""
                            };
                            this.newRole = {};
                            this.loadApplications();
                        });
                    },
                    getContextRoles(context) {
                        for( let r = 0 ; r < this.application.roles.length; r++) {
                            if(this.application.roles[r].context == context) return this.application.roles[r];
                        }
                        let ctx = {context: context, roles: []};
                        this.application.roles.push(ctx);
                        return ctx;
                    },
                    deleteApp(application) {
                        if (confirm("Delete application: " + application.name)) {
                            axios.delete('v1/applications/' + encodeURIComponent(application.name))
                                    .then(response => {
                                        this.feedback = application.name + " deleted";
                                        this.application = {roles: [], workingRoles: []};
                                        this.newRole = {};
                                        this.loadApplications();
                                    });
                        }
                    },

                    renewToken(application) {
                        if (confirm("Generate new token for application: " + application.name)) {
                            axios.patch('v1/applications/' + encodeURIComponent(application.name))
                                    .then(response => {
                                        this.feedback = application.name + ": Application Token: " + response.data;
                                    });
                        }
                    },

                    selectApp(app) {
                        this.application = {
                            name: app.name,
                            description: app.description,
                            metaData: app.metaData,
                            metaDataLabel: "",
                            metaDataValue: "",
                            workingRoles: app.workingRoles.slice()
                        };
                        this.feedback = "";
                    },
                    connect() {
                        let me = this;
                        me.webSocketInfo.subscribe('/topic/application', function () {
                            me.loadApplications();
                        }, function(sub) {
                            me.subscription = sub;
                        });
                    },
                    addNewRole() {
                        if (!this.existsNewRole()) {
                            this.application.workingRoles.push(this.newRole);
                            this.newRole = {}
                        }
                    },
                    deleteContextRole(idx) {
                        let newArr = [];
                        for (let a = 0; a < this.application.workingRoles.length; a++) {
                            if (a != idx) {
                                newArr.push(this.application.workingRoles[a]);
                            }
                        }
                        this.application.workingRoles = newArr;
                    },
                    addContextRole() {
                        if (this.newRole.context && this.newRole.role) {
                            if (!this.existsNewRole()) {
                                this.application.workingRoles.push(this.newRole);
                                this.newRole = {};
                            } else {
                                alert("Role already assigned for context");
                            }
                        } else {
                            alert("Select role and context to add");
                        }
                    },
                existsNewRole() {
                    for (var a = 0; a < this.application.workingRoles.length; a++) {
                        if (this.application.workingRoles[a].context == this.newRole.context &&
                                this.application.workingRoles[a].role == this.newRole.role) {
                            return true;
                        }
                    }
                    return false;
                },
                addMetaData() {
                    this.application.metaData[this.application.metaDataLabel] = this.application.metaDataValue;
                    this.application.metaDataLabel = '';
                    this.application.metaDataValue = '';
                },
                deleteMetaData(key) {
                    delete this.application.metaData[key];
                    this.$forceUpdate();
                }

            }
            });
