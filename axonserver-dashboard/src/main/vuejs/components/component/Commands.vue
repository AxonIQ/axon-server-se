<!--
  - Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
  - under one or more contributor license agreements.
  -
  -  Licensed under the AxonIQ Open Source License Agreement v1.0;
  -  you may not use this file except in compliance with the license.
  -
  -->

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
                axios.get("v1/components/"+encodeURI(this.component)+"/commands?context=" + this.context).then(response => {
                    this.commands = response.data;
                });
            }
        }
    }
</script>

<style scoped>
</style>