//# sourceURL=application.js
globals.pageView = new Vue(
        {
            el: '#application',
            data: {
                license: "",
                application: {workingRoles: []},
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
                        axios.get("v1/public/applications").then(response => {
                            this.applications = response.data;

                            for( let a = 0 ; a < this.applications.length; a++ ) {
                                let app = this.applications[a];
                                app.workingRoles = [];
                                for (let c = 0; c < app.roles.length; c++) {
                                    let ctx = app.roles[c];
                                    for (let r = 0; r < ctx.roles.length; r++) {
                                        app.workingRoles.push({"context": ctx.context, "role": ctx.roles[r]});
                                    }
                                }
                            }
                        });
                    },

                    saveApp(application) {
                        if (this.newRole.context && this.newRole.role) {
                            this.addNewRole(this.newRole);
                        }
                        this.application.roles = [];
                        for( let r = 0 ; r < this.application.workingRoles.length; r++) {
                            let context = this.getContextRoles(this.application.workingRoles[r].context);
                            context.roles.push(this.application.workingRoles[r].role);
                        }

                        this.application.workingRoles = null
                        axios.post('v1/applications', this.application).then(response => {
                            this.feedback = application.name + ": Application Token: " + response.data;
                            this.application = {roles: [], workingRoles: []};
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


                        this.application = {name: app.name, description: app.description, workingRoles: app.workingRoles.slice()};
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
                    }
                }
            });
