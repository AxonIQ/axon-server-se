/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

import Vue from 'vue'
import VModal from 'vue-js-modal'
import VClipboard from 'vue-clipboard2'
import ComponentInstances from './components/component/Instances.vue';
import ComponentCommands from './components/component/Commands.vue';
import ComponentQueries from './components/component/Queries.vue';
import ComponentProcessors from './components/component/Processors.vue';
import ComponentCommandMetrics from './components/component/CommandMetrics.vue';
import ComponentQueryMetrics from './components/component/QueryMetrics.vue';
import ComponentSubscriptionsMetrics from './components/component/SubscriptionsMetrics.vue';
import SuggestList from './components/component/SuggestList.vue';
import Pagination from './components/component/Pagination.vue';
import PaginatedTable from './components/component/PaginatedTable.vue';

Vue.use(VModal)
Vue.use(VClipboard)

Vue.component('component-instances', ComponentInstances);
Vue.component('component-commands', ComponentCommands);
Vue.component('component-queries', ComponentQueries);
Vue.component('component-processors', ComponentProcessors);
Vue.component('component-command-metrics', ComponentCommandMetrics);
Vue.component('component-query-metrics', ComponentQueryMetrics);
Vue.component('component-subscription-metrics', ComponentSubscriptionsMetrics);
Vue.component('pagination', Pagination);
Vue.component('paginated-table', PaginatedTable);


Vue.component('component-suggestion-box', SuggestList);

window.Vue = Vue;
