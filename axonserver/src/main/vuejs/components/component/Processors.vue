<template>
    <div id="component-processors">
        <section style="display:block; float: left; width: 65%">
            <div class="results singleHeader">
                <table>
                    <colgroup>
                        <col width="15%">
                        <col width="15%">
                        <col width="15%">
                        <col width="10%">
                        <col width="15%">
                        <col width="30%">
                    </colgroup>
                    <thead>
                    <tr>
                        <th><div>Processor Name</div></th>
                        <th><div>Processing Mode</div></th>
                        <th><div>Active Threads</div></th>
                        <th><div></div></th>
                        <th v-if="hasFeature('AUTOMATIC_TRACKING_PROCESSOR_SCALING_BALANCING')" style="text-align: center">Auto Load Balancing</th>
                        <th><div></div></th>
                    </tr>
                    </thead>
                    <tbody class="selectable">
                    <tr v-for="processor in processors" :class="{selected : processor.name === selected.name}"
                        @click="select(processor)">
                        <td><div>{{processor.name}}</div></td>
                        <td><div>{{processor.mode}}</div></td>
                        <td>{{processor.activeThreads}}</td>
                        <td>
                            <span :class="{hidden : !processor.loading}"><i class="fas fa-spinner fa-pulse"></i></span>
                            <span :class="{hidden : !processor.canPause}" @click="pauseProcessor(processor)"><i
                                    class="far fa-pause-circle fa-lg"></i></span>
                            <span :class="{hidden : !processor.canPlay}" @click="startProcessor(processor)"><i
                                    class="far fa-play-circle fa-lg"></i></span>
                            <span  v-if="processor.mode === 'Tracking'" @click="showLoadBalance(processor)"><i class="fas fa-balance-scale fa-lg"></i></span>

                        </td>
                        <td v-if="hasFeature('AUTOMATIC_TRACKING_PROCESSOR_SCALING_BALANCING')" align="center">
                            <span v-if="processor.mode === 'Tracking'">
                                <select v-model="processorsLBStrategies[processor.name]"
                                        @change="changeLoadBalancingStrategy(processor.name, processorsLBStrategies[processor.name])">
                                    <option v-for="strategy in loadBalancingStrategies" :value="strategy.name">{{strategy.label}}</option>
                                </select>
                            </span>
                        </td>
                        <td>
                            <span v-for="warning in processor.warnings"><i class="fas fa-exclamation-triangle"></i> {{warning.message}}<br></span>
                        </td>

                    </tr>
                    </tbody>
                </table>
            </div>
        </section>
        <section v-if="selected.name" style="display:block; float: left; width: 30%; margin-left: 5%">
            <div class="results singleHeader">

                <table class="nodes" v-if="selected.mode === 'Tracking'">
                    <colgroup>
                        <col width="20%">
                        <col width="40%">
                        <col width="15%">
                        <col width="15%">
                    </colgroup>
                    <thead>
                    <tr>
                        <th><div>Segment</div></th>
                        <th><div>ClientId</div></th>
                        <th><div>Size</div></th>
                    </tr>
                    </thead>
                    <tbody>
                    <tr v-for="tracker in selected.trackers">
                        <td><div>{{tracker.segmentId}}</div></td>
                        <td><div>{{tracker.clientId}}</div></td>
                        <td>1/{{tracker.onePartOf}}</td>
                        <td>
                            <i v-if="!tracker.caughtUp" class="fas fa-sign-in-alt" style="color: red"></i>
                            <i v-if="tracker.replaying" class="fas fa-history" style="color: red"></i>
                            <span v-if="canMoveFrom(tracker.clientId)" @click="showMoveSegment(tracker)" title="Move">
                                <i class="fas fa-people-carry"></i>
                            </span>
                        </td>
                    </tr>

                    </tbody>
                </table>
            </div>
        </section>
        <modal name="move-segment" width="500" height="250">
            <div id="move-segment" class="column configuration modal">
                <h2>Move Segment {{movingSegment.segmentId}} </h2>

                <form>
                    <ul>
                        <li>
                            <span>From client</span>
                            <span><input disabled v-model="movingSegment.clientId"/></span>
                        </li>
                        <li>
                            <span>To client</span>
                            <span> <select v-model="segmentDestination">
                                        <option selected disabled value="">Please select one</option>
                                        <option v-for="client in segmentDestinationOptions">{{client}}</option>
                                    </select>
                            </span>
                        </li>
                        <li>
                            <span>&nbsp;</span>
                            <span>
                                <button @click.prevent="hideMoveSegment()" class="button">Cancel</button>
                                <button @click.prevent="moveSegment()" class="button">Move</button>
                            </span>
                        </li>
                    </ul>
                </form>

            </div>
        </modal>
        <modal name="load-balance" width="500" height="250">
            <div id="load-balance" class="column configuration modal">
                <h2>Load Balance {{loadBalanceProcessor.name}} Event Processor</h2>
                <form>
                    <ul>
                        <li>
                            <span>Using strategy</span>
                            <span>
                                <select v-model="loadBalanceStrategy">
                                    <option v-for="strategy in loadBalancingStrategies" :value="strategy.name">{{strategy.label}}</option>
                                </select>
                            </span>
                        </li>
                        <li>
                            <span>&nbsp;</span>
                            <span class="button-bar">
                                <button @click.prevent="hideLoadBalance()" class="button">Cancel</button>
                                <button @click.prevent="loadBalance()" class="button">Balance</button>
                            </span>
                        </li>
                    </ul>
                </form>

            </div>
        </modal>
    </div>

</template>

<script>
    module.exports = {
        name: 'component-processors',
        props: ['component', 'context'],
        data() {
            return {
                processors: [],
                processorsLBStrategies: {},
                selected: {},
                segmentDestinationOptions: [],
                movingSegment: "",
                segmentDestination: "",
                loadBalanceProcessor: {},
                loadBalanceStrategy: "DEFAULT",
                loadBalancingStrategies: [],
                subscriptions: [],
                webSocketInfo: globals.webSocketInfo
            }
        }, mounted() {
            this.loadComponentProcessors();
            this.loadLBStrategies();
            let me = this;
            me.webSocketInfo.subscribe('/topic/processor', this.loadComponentProcessors, function (sub) {
                me.subscriptions.push(sub);
            });
            me.webSocketInfo.subscribe('/topic/cluster', this.loadComponentProcessors, function (sub) {
                me.subscriptions.push(sub);
            })
        }, beforeDestroy() {
            this.subscriptions.forEach(sub => sub.unsubscribe());
            // if (this.subscription) {
            //     this.subscription.unsubscribe();
            // }
        }, methods: {
            showMoveSegment(tracker) {
                axios.get("v1/components/" + this.component + "/instances?context=" + this.context).then(response => {
                    this.segmentDestinationOptions = response.data
                            .map(instance => instance.name)
                            .filter(instanceName => instanceName !== tracker.clientId);
                    this.movingSegment = tracker;
                    this.$modal.show('move-segment');
                });
            },
            hideMoveSegment() {
                this.moveSegmentInstances = [];
                this.segment = "";
                this.$modal.hide('move-segment');
            },
            moveSegment() {
                axios.patch("v1/components/" + this.component
                                    + "/processors/" + this.selected.name
                                    + "/segments/" + this.movingSegment.segmentId
                                    + "/move?target=" + this.segmentDestination
                                    + "&context=" + this.context).then(
                        response => {
                            this.hideMoveSegment();
                        }
                );
            },
            select(processor) {
                this.selected = processor;
                this.selected.trackers = processor.trackers.sort((a, b) => a.segmentId - b.segmentId);
            },
            loadComponentProcessors() {
                axios.get("v1/components/" + this.component + "/processors?context=" + this.context).then(response => {
                    this.processors = response.data;
                    if (this.selected.name) {
                        for (let processor of this.processors) {
                            if (processor.name === this.selected.name) {
                                this.select(processor);
                            }
                        }
                    }
                });
            },
            loadLBStrategies(){
                axios.get("v1/processors/loadbalance/strategies").then(response => {
                    this.loadBalancingStrategies = response.data;
                });
                axios.get("v1/components/" + this.component + "/processors/loadbalance/strategies?context=" + this.context).then(response => {
                    this.processorsLBStrategies = response.data;
                });
            },
            startProcessor(processor) {
                if (confirm("Start processor " + processor.name + "?")) {
                    axios.patch("v1/components/" + this.component + "/processors/" + processor.name + "/start?context=" + this.context).then(
                            response => {
                                this.enableStatusLoader(processor);
                            }
                    );
                }
            },
            pauseProcessor(processor) {
                if (confirm("Pause processor " + processor.name + "?")) {
                    axios.patch("v1/components/" + this.component + "/processors/" + processor.name + "/pause?context=" + this.context).then(
                            response => {
                                this.enableStatusLoader(processor);
                            }
                    );
                }
            },
            showLoadBalance(processor){
                this.loadBalanceProcessor = processor;
                this.$modal.show('load-balance');
            },
            hideLoadBalance(){
                this.loadBalanceProcessor = {};
                this.loadBalanceStrategy = "DEFAULT";
                this.$modal.hide('load-balance');
            },
            loadBalance(){
                axios.patch("v1/components/" + this.component + "/processors/" + this.loadBalanceProcessor.name +
                                    "/loadbalance?context=" + this.context + "&strategy=" + this.loadBalanceStrategy).then(
                        response => {
                            this.enableStatusLoader(this.loadBalanceProcessor);
                            this.$modal.hide('load-balance');
                        }
                );
            },
            enableStatusLoader(processor) {
                processor.loading = true;
                processor.canPause = false;
                processor.canPlay = false;
            },
            canMoveFrom(clientId) {
                let freeThreadInstances = this.selected.freeThreadInstances;
                return freeThreadInstances.filter(value => value !== clientId).length > 0;
            },
            changeLoadBalancingStrategy(processor, strategy){
                console.log("strategy changed 5: "+ strategy);
                axios.put("v1/components/" + this.component + "/processors/" + processor +
                                  "/loadbalance?context=" + this.context + "&strategy="+ strategy).then(response => {
                    this.loadLBStrategies();
                });
            }
        }
    }
</script>

<style scoped>
    .hidden {
        display: none;
    }

    .modal {
        padding: 20px;
    }

    .modal .button-bar {
        margin-top: 20px;
    }
</style>