//# sourceURL=users.js
globals.pageView = new Vue(
        {
            el: '#users',
            data: {
                roles: [],
                user: {roles: []},
                users: [],
                feedback: "",
                webSocketInfo: globals.webSocketInfo,
                admin: globals.admin
            }, mounted() {
                this.loadRoles();
                this.loadUsers();
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
                            });
                },

                save(user) {
                    this.feedback = "";
                    if( ! this.user.userName ) {
                        alert("Please enter name for user");
                        return;
                    }
                    if( this.user.roles.length === 0) {
                        alert("Please select roles for user");
                        return;
                    }
                    if( ! this.user.password && !this.existsUser(this.user.userName)) {
                        alert("Please enter password for new user");
                        return;
                    }

                    axios.post('v1/users', user)
                            .then(() => {
                                this.feedback = "User saved ";
                                this.user = {roles: []};
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
                    this.user = {userName: u.userName, roles: u.roles};
                    this.feedback = "";
                },

                connect() {
                    let me = this;
                    me.webSocketInfo.subscribe('/topic/user', function () {
                        me.loadUsers();
                    }, function(sub) {
                        me.subscription = sub;
                    });
                }
            }
        });