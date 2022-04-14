<!--
  -  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
  -  under one or more contributor license agreements.
  -
  -  Licensed under the AxonIQ Open Source License Agreement v1.0;
  -  you may not use this file except in compliance with the license.
  -
  -->

<template>
  <span style="width: 100%">
  <vue-simple-suggest :value="value"
                      :list="simpleSuggestionList"
                      :filter-by-query="true"
                      display-attribute="key"
                      value-attribute="key"
                      @input="updateValue($event)"
                      >
    <input data-lpignore="true" ref="suggested-value" autocomplete="none"
           style="padding: 1px 2px;height: 21px;border-color: #888;"/>
    <div slot="suggestion-item" slot-scope="{ suggestion, query }">
      <span style="width: 100%">{{ suggestion.key }} - {{ suggestion.description }}</span>
    </div>
  </vue-simple-suggest>
    </span>
</template>
<script>
import VueSimpleSuggest from 'vue-simple-suggest'
import 'vue-simple-suggest/dist/styles.css' // Optional CSS

export default {
  name: 'component-suggestion-box',
  components: {
    VueSimpleSuggest
  },
  props: ['options', 'value'],
    methods: {
      simpleSuggestionList() {
        return this.options
      },
      updateValue(ev) {
        this.$emit('input', ev)
      },
      setText(text) {
        this.$children[0].setText(text);
      }
    }
  }
</script>
