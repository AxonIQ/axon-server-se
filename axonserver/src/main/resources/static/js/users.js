/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

//# sourceURL=users.js
globals.pageView = new Vue(
        {
            el: '#users',
            data: {
                roles: [],
                user: {workingRoles: []},
                users: [],
                newRole: {},
                contexts: [],
                feedback: "",
                webSocketInfo: globals.webSocketInfo,
                admin: globals.admin
            }, mounted() {
                this.loadRoles();
                this.loadUsers();
                axios.get("v1/public/context").then(response => {
                    this.contexts = response.data;
                    this.contexts.push({context: "*"});
                });
                this.connect();
            }, beforeDestroy() {
                if( this.subscription) this.subscription.unsubscribe();
            }, methods: {
                loadRoles() {
                    axios.get("v1/roles/user")
                            .then(response => {
                                this.roles = response.data;
                            });
                },

                loadUsers() {
                    axios.get("v1/public/users")
                            .then(response => {
                                this.users = response.data;
                                for (let a = 0; a < this.users.length; a++) {
                                    let app = this.users[a];
                                    app.workingRoles = [];
                                    for (let c = 0; c < app.roles.length; c++) {
                                        let ctx = app.roles[c].split("@", 2);
                                        app.workingRoles.push({"context": ctx[1], "role": ctx[0]});
                                    }
                                }
                            });
                },

                save(user) {
                    this.feedback = "";
                    if( ! this.user.userName ) {
                        alert("Please enter name for user");
                        return;
                    }
                    if (this.user.workingRoles.length === 0) {
                        alert("Please select roles for user");
                        return;
                    }
                    if( ! this.user.password && !this.existsUser(this.user.userName)) {
                        alert("Please enter password for new user");
                        return;
                    }

                    if (this.newRole.context && this.newRole.role) {
                        this.addNewRole(this.newRole);
                    }
                    this.user.roles = [];
                    for (let r = 0; r < this.user.workingRoles.length; r++) {
                        let role = this.user.workingRoles[r];
                        this.user.roles.push(role.role + "@" + role.context);
                    }

                    this.user.workingRoles = null

                    axios.post('v1/users', user)
                            .then(() => {
                                this.feedback = "User saved ";
                                this.user = {roles: [], workingRoles: []};
                                this.newRole = {};
                                this.loadUsers();
                            });

                },

                existsUser(name) {
                    for( let i = 0 ; i < this.users.length; i++) {
                        if( this.users[i].userName === name) return true;
                    }
                    return false;
                },

                deleteUser(u) {
                    this.feedback = "";
                    if (confirm("Delete user: " + u.userName)) {
                        axios.delete('v1/users/' + encodeURIComponent(u.userName))
                                .then( () => {
                                    this.feedback = u.userName + " deleted";
                                    this.user = {roles: []};
                                    this.loadUsers();
                                }
                        );
                    }
                },

                selectUser(u) {
                    this.user = {userName: u.userName, workingRoles: u.workingRoles.slice()};
                    this.feedback = "";
                },

                connect() {
                    let me = this;
                    me.webSocketInfo.subscribe('/topic/user', function () {
                        me.loadUsers();
                    }, function(sub) {
                        me.subscription = sub;
                    });
                },

                deleteContextRole(idx) {
                    let newArr = [];
                    for (let a = 0; a < this.user.workingRoles.length; a++) {
                        if (a != idx) {
                            newArr.push(this.user.workingRoles[a]);
                        }
                    }
                    this.user.workingRoles = newArr;
                },
                addNewRole() {
                    if (!this.existsNewRole()) {
                        this.user.workingRoles.push(this.newRole);
                        this.newRole = {}
                    }
                },
                addContextRole() {
                    if (this.newRole.context && this.newRole.role) {
                        if (!this.existsNewRole()) {
                            this.user.workingRoles.push(this.newRole);
                            this.newRole = {};
                        } else {
                            alert("Role already assigned for context");
                        }
                    } else {
                        alert("Select role and context to add");
                    }
                },
                existsNewRole() {
                    for (var a = 0; a < this.user.workingRoles.length; a++) {
                        if (this.user.workingRoles[a].context == this.newRole.context &&
                                this.user.workingRoles[a].role == this.newRole.role) {
                            return true;
                        }
                    }
                    return false;
                }
            }
        });