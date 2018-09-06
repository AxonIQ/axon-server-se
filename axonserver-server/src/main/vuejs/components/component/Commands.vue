<template>
    <div id="component-commands" class="results singleHeader">
        <table>
            <thead>
            <tr>
                <th>Command</th>
            </tr>
            </thead>
            <tbody class="selectable">
            <tr v-for="command in commands">
                <td>{{command.name}}</td>
            </tr>
            </tbody>
        </table>
    </div>
</template>


<script>
    module.exports = {
        name: 'component-commands',
        props:['component', 'context'],
        data(){
            return {
                commands: []
            }
        }, mounted() {
            this.loadComponentCommands();
        }, methods: {
            loadComponentCommands(){
                axios.get("v1/components/"+this.component+"/commands?context=" + this.context).then(response => {
                    this.commands = response.data;
                });
            }
        }
    }
</script>

<style scoped>
</style>