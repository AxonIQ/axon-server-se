//# sourceURL=application.js
globals.pageView = new Vue(
        {
            el: '#application',
            data: {
                license: "",
                application: {roles: []},
                applications: [],
                contexts: [],
                appRoles: [],
                newRole: {},
                feedback: "",
                webSocketInfo: globals.webSocketInfo,
                admin: globals.admin
            }, computed: {}, mounted() {
                axios.get("v1/public/license").then(response => this.license = response.data.edition.toLowerCase());
                axios.get("v1/roles/application").then(response => this.appRoles = response.data);
                axios.get("v1/public/context").then(response => this.contexts = response.data);
                this.loadApplications();
                this.connect();
            }, beforeDestroy() {
                if( this.subscription) this.subscription.unsubscribe();
            }, methods: {
                    loadApplications() {
                        axios.get("v1/public/applications").then(response => this.applications = response.data);
                    },

                    saveApp(application) {
                        if (this.newRole.context && this.newRole.role) {
                            if (!this.existsNewRole()) {
                                this.application.roles.push(this.newRole);
                                this.newRole = {};
                            }
                        }

                        axios.post('v1/applications', application).then(response => {
                            this.feedback = application.name + ": Application Token: " + response.data;
                            this.application = {roles: []};
                            this.newRole = {};
                            this.loadApplications();
                        });
                    },

                    deleteApp(application) {
                        if (confirm("Delete application: " + application.name)) {
                            axios.delete('v1/applications/' + encodeURIComponent(application.name))
                                    .then(response => {
                                        this.feedback = application.name + " deleted";
                                        this.application = {roles: []};
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
                        this.application = {name: app.name, description: app.description, roles: app.roles.slice()};
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
                    deleteContextRole(idx) {
                        let newArr = [];
                        for (a = 0; a < this.application.roles.length; a++) {
                            if (a != idx) {
                                newArr.push(this.application.roles[a]);
                            }
                        }
                        this.application.roles = newArr;
                    },
                    addContextRole() {
                        if (this.newRole.context && this.newRole.role) {
                            if (!this.existsNewRole()) {
                                this.application.roles.push(this.newRole);
                                this.newRole = {};
                            } else {
                                alert("Role already assigned for context");
                            }
                        } else {
                            alert("Select role and context to add");
                        }
                    },
                    existsNewRole() {
                        for (var a = 0; a < this.application.roles.length; a++) {
                            if (this.application.roles[a].context == this.newRole.context &&
                                    this.application.roles[a].role == this.newRole.role) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
            });
