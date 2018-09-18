<template>
    <div id="component-command-metrics" class="content results">
        <table id="commands" class="table table-striped">
            <thead>
            <tr>
                <th class="commandCol">Commands</th>
                <th v-for="clientId in clients">
                    <div>{{componentNames[clientId]}} <br>
                        <small>{{clientId}}</small>
                    </div>
                </th>
            </tr>
            </thead>
            <tbody id="statistics">
            <tr v-for="commandName in commands">
                <td><div>{{commandName}}</div></td>
                <td v-for="clientId in clients">{{metrics[clientId][commandName] || 0}}</td>
            </tr>
            </tbody>
        </table>
    </div>
</template>

<script>
    module.exports = {
        name: 'component-command-metrics',
        props: [],
        data: function() {
            return {
                clients: [],
                componentNames: [],
                commands: [],
                metrics: [],
                webSocketInfo: globals.webSocketInfo
            }
        }, mounted: function() {
            axios.get("v1/public/command-metrics").then(response => {
                response.data.forEach(m => {
                    this.loadMetric(m);
                });
            });
            this.connect(this);
        }, beforeDestroy: function() {
            if( this.subscription) this.subscription.unsubscribe();
        }, methods: {
            loadMetric(m) {
                if (this.clients.indexOf(m.clientId) === -1) {
                    this.clients.push(m.clientId);
                    Vue.set(this.componentNames, m.clientId, m.componentName);
                }
                if (this.commands.indexOf(m.command) === -1) {
                    this.commands.push(m.command);
                }

                if (!this.metrics[m.clientId]) {
                    Vue.set(this.metrics, m.clientId, []);
                }
                Vue.set(this.metrics[m.clientId], m.command, m.count);

            },
            connect(me) {
                me.webSocketInfo.subscribe('/topic/commands', function (metric) {
                    me.loadMetric(JSON.parse(metric.body));
                }, function(subscription) {
                    me.subscription = subscription;
                    console.info( me.name + " - " + me.subscription);
                });
            }
        }
    };
</script>