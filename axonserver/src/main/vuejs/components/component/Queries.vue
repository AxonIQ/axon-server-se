<template>
    <div id="component-queries" class="results singleHeader">
        <table>
            <thead>
            <tr>
                <th>Query</th>
                <th>Response Types</th>
                <th>#Subscriptions</th>
                <th>#Active Subscriptions </th>
                <th>#Updates</th>
            </tr>
            </thead>
            <tbody class="selectable">
            <tr v-for="query in queries">
                <td><div>{{query.name}}</div></td>
                <td><div v-for="response in query.responseTypes">{{response}}</div></td>
                <td><div>{{query.metrics.totalSubscriptions}}</div></td>
                <td><div>{{query.metrics.activeSubscriptions}}</div></td>
                <td><div>{{query.metrics.updates}}</div></td>
            </tr>
            </tbody>
        </table>
    </div>
</template>


<script>
    module.exports = {
        name: 'component-queries',
        props:['component', 'context'],
        data(){
            return {
                queries: []
            }
        }, mounted() {
            this.loadComponentQueries();
        }, methods: {
            loadComponentQueries(){
                var baseUrl = "v1/components/"+this.component;
                let self = this;
                axios.get(baseUrl+"/queries?context=" + this.context).then(response => {
                    var queries = response.data;
                    queries.forEach(function (query, index) {
                        axios.get(baseUrl+"/subscription-query-metric/query/"+query.name+"?context=" + self.context)
                                .then(metric => {
                                    query.metrics = metric.data;
                                    self.queries.push(query);
                                });
                    })
                });
            }
        }
    }
</script>

<style scoped>
</style>