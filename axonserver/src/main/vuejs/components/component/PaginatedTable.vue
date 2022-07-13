<!--
  -  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
  -  under one or more contributor license agreements.
  -
  -  Licensed under the AxonIQ Open Source License Agreement v1.0;
  -  you may not use this file except in compliance with the license.
  -
  -->
<template>
  <div class="results singleHeader">
    <table>
      <thead>
      <tr>
        <slot name="header"></slot>
      </tr>
      </thead>
      <tbody :class="clazz">
      <tr v-for="n in wrappedRows.visibleRows()">
        <slot name="row" v-bind="n"></slot>
      </tr>
      </tbody>
    </table>
    <pagination :data="wrappedRows"></pagination>
    <br/>
  </div>
</template>
<script>
module.exports = {
  name: 'paginated-table',
  props: ['rows', 'selectable', 'page'],
  data() {
    return {
      wrappedRows: newPagedArray(),
      clazz: ""
    }
  },
  watch: {
    rows: function (newValue) {
      this.wrappedRows = this.wrappedRows.withRows(newValue);
    },
  },
  mounted() {
    if (this.selectable) {
      this.clazz = "selectable";
    }
    if (this.page) {
      this.wrappedRows = new PagedArray([], this.page, 1);
    }
    if (this.rows) {
      this.wrappedRows = this.wrappedRows.withRows(this.rows);
    }
  },
  methods: {}
}
</script>