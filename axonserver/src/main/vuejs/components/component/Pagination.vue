<!--
  -  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
  -  under one or more contributor license agreements.
  -
  -  Licensed under the AxonIQ Open Source License Agreement v1.0;
  -  you may not use this file except in compliance with the license.
  -
  -->

<template>
  <div class="pager">
    Rows per page
    <select v-model="data.pageSize" @change="pageSizeChanged()">
      <option value="5">5</option>
      <option value="10">10</option>
      <option value="15">15</option>
      <option value="-1">All</option>
    </select>
    {{ first }} - {{ last }} of {{ data.rows.length }}

    <button :disabled="!hasPrevious()" @click="prevPage()">&lt;</button>
    <button :disabled="!hasNext()" @click="nextPage()">&gt;</button>
  </div>
</template>
<script>
module.exports = {
  name: 'pagination',
  props: ['data'],
  data() {
    return {
      first: 0,
      last: 0
    }
  },
  watch: {
    data: function () {
      this.setRange();
    }
  },
  mounted() {
    this.setRange();
  },
  methods: {
    currentPage() {
      return this.data.currentPage;
    },
    pages() {
      if (this.data.pageSize > 0) {
        return Math.ceil(this.data.rows.length / this.data.pageSize);
      }
      return 1;
    },
    nextPage() {
      if (this.data.pageSize > 0) {
        if (this.data.rows.length > this.data.currentPage * this.data.pageSize) {
          this.data.currentPage++;
          this.setRange();
        }
      }
    },
    prevPage() {
      if (this.data.currentPage > 1) {
        this.data.currentPage--;
        this.setRange();
      }
    },
    pageSizeChanged() {
      this.data.currentPage = 1;
      this.setRange();
    },
    setRange() {
      if (this.data.rows.length === 0) {
        this.first = 0;
      } else {
        this.first = ((this.data.currentPage - 1) * this.data.pageSize) + 1;
      }
      if (this.data.pageSize < 0) {
        this.last = this.data.rows.length;
      } else {
        this.last = Math.min(this.data.rows.length, this.first + parseInt(this.data.pageSize) - 1);
      }
    },
    hasPrevious() {
      return this.first > 1;
    },
    hasNext() {
      return this.last < this.data.rows.length;
    }
  }
}
</script>