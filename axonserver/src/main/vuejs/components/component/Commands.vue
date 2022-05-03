<!--
  -  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
  -  under one or more contributor license agreements.
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
      <tbody>
      <tr v-for="command in commands">
        <td>{{ command.name }}</td>
      </tr>
      </tbody>
    </table>
  </div>
</template>


<script>
module.exports = {
  name: 'component-commands',
  props: ['component', 'context'],
  data() {
    return {
      webSocketInfo: globals.webSocketInfo,
      commands: []
    }
  }, mounted() {
    this.loadComponentCommands();
    let me = this;
    this.webSocketInfo.subscribe('/topic/cluster', me.loadComponentCommands, function (sub) {
      me.subscription = sub;
    });
  }, beforeDestroy() {
    if (this.subscription) {
      this.subscription.unsubscribe();
    }
  }, methods: {
    loadComponentCommands() {
      axios.get("v1/components/" + encodeURIComponent(this.component) + "/commands?context=" + this.context).then(response => {
        this.commands = response.data.filter((item, pos, self) => self.findIndex(v => v.name === item.name) === pos);
      });
    }
  }
}
</script>

<style scoped>
</style>