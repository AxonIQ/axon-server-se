/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

/**
 * Enum of AxonServer metric names.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public enum BaseMetricName implements MetricName {

    COMMAND_THROUGHPUT("axon.commands.throughput", "Number of commands"),
    COMMAND_DURATION("axon.commands.duration",
                     "Duration of commands, from received by Axon Server until response sent to requester"),
    COMMAND_HANDLING_DURATION("axon.commands.duration.handling",
                              "Duration of commands, from sent to handler until response received from handler"),
    COMMAND_ERRORS("axon.commands.error.count", "Number of errors"),
    COMMAND_QUEUED("axon.commands.saturation.queued.count", "Number of commands queued in Axon Server"),
    COMMAND_ACTIVE("axon.commands.saturation.active.count", "Number of in-flight commands"),

    QUERY_THROUGHPUT("axon.queries.throughput", "Number of queries"),
    QUERY_DURATION("axon.queries.duration",
                   "Duration of queries, from received by Axon Server until response sent to requester"),
    QUERY_HANDLING_DURATION("axon.queries.duration.handling",
                            "Duration of queries, from sent to handler until response received from handler"),
    QUERY_ERRORS("axon.queries.error.count", "Number of errors"),
    QUERY_QUEUED("axon.queries.saturation.queued.count", "Number of queries queued in Axon Server"),
    QUERY_ACTIVE("axon.queries.saturation.active.count", "Number of in-flight queries"),

    APPEND_EVENT_THROUGHPUT("axon.events.append.throughput", "Number of events appended"),
    APPEND_EVENT_DURATION("axon.events.append.duration",
                          "Duration of append event request, from the first event in a transaction received by Axon Server until the transaction is completed"),
    APPEND_EVENT_ERRORS("axon.events.append.errors", "Number of errors"),
    APPEND_EVENT_ACTIVE("axon.events.append.active", "Number of active append event transactions"),

    READ_AGGREGATE_EVENTS_THROUGHPUT("axon.events.read.aggregate.throughput", "Number of aggregates read"),
    READ_AGGREGATE_EVENTS_DURATION("axon.events.read.aggregate.duration", "Duration of read aggregate request"),
    READ_AGGREGATE_EVENTS_ERRORS("axon.events.read.aggregate.errors", "Number of errors"),
    READ_AGGREGATE_EVENTS_ACTIVE("axon.events.read.aggregate.active", "Number of active aggregate read actions"),

    APPEND_SNAPSHOT_THROUGHPUT("axon.snapshots.append.throughput", "Number of events appended"),
    APPEND_SNAPSHOT_DURATION("axon.snapshots.append.duration",
                             "Duration of append event request, from the first event in a transaction received by Axon Server until the transaction is completed"),
    APPEND_SNAPSHOT_ERRORS("axon.snapshots.append.errors", "Number of errors"),
    APPEND_SNAPSHOT_ACTIVE("axon.snapshots.append.active", "Number of active append event transactions"),

    READ_SNAPSHOT_THROUGHPUT("axon.snapshots.read.throughput", "Number of aggregates read"),
    READ_SNAPSHOT_DURATION("axon.snapshots.read.duration", "Duration of read aggregate request"),
    READ_SNAPSHOT_ERRORS("axon.snapshots.read.errors", "Number of errors"),
    READ_SNAPSHOT_ACTIVE("axon.snapshots.read.active", "Number of active aggregate read actions"),

    EVENTSTORE_FORCE_LAG("axon.eventstore.saturation.force.lag", "Number of unforced entries"),
    EVENTSTORE_FORCE_DURATION("axon.eventstore.duration.force", "Duration of a force to disk"),
    APPLICATION_CONNECT_DURATION("axon.applications.duration.connection", "Duration of application connections"),
    APPLICATION_CONNECT("axon.applications.throughput.connect.count", "Number of application connect requests"),
    APPLICATION_CONNECTED("axon.applications.saturation.connected.count", "Number of applications currently connected"),
    APPLICATION_DISCONNECT("axon.applications.throughput.disconnect.count",
                           "Number of application disconnect requests"),
    AUTHENTICATION_ERRORS("axon.authentication.errors.count", "Number of authentication errors"),


    /**
     * Metric for number of commands handled by a client (tags: source client/handler client/context/command)
     */
    @Deprecated AXON_COMMAND("axon.commands", "Number of commands handled by the Axon Server node"),
    /**
     * Metric for the rate of commands handled (tags: context)
     */
    @Deprecated AXON_COMMAND_RATE("axon.command", "Number of commands"),
    /**
     * Metric for the number of currently active commands
     */
    @Deprecated AXON_ACTIVE_COMMANDS("axon.commands.active", "Number of pending commands"),
    /**
     * Metric for number of queries handled by a client (tags: source client/handler client/context/query)
     */
    @Deprecated AXON_QUERY("axon.queries", "Number of queries handled by the Axon Server node"),
    /**
     * Metric for the rate of queries handled (tags: context)
     */
    @Deprecated AXON_QUERY_RATE("axon.query", "Number of queries"),
    /**
     * Metric for the number of currently active queries
     */
    @Deprecated AXON_ACTIVE_QUERIES("axon.queries.active", "Number of pending queries"),
    /**
     * Metric for the token of the last event in the event store (tags: context)
     */
    AXON_EVENT_LAST_TOKEN("axon.event.lastToken", "Last token in the event store"),
    /**
     * Metric for the token of the last snapshot in the event store (tags: context)
     */
    AXON_SNAPSHOT_LAST_TOKEN("axon.snapshot.lastToken", "Last snapshot token in the event store"),
    /**
     * Metric for the rate of events stored (tags: context)
     */
    @Deprecated AXON_EVENTS("axon.event", "Number of event stored per second"),
    /**
     * Metric for the rate of snapshots stored (tags: context)
     */
    @Deprecated AXON_SNAPSHOTS("axon.snapshot", "Number of snapshots stored"),
    @Deprecated AXON_GLOBAL_SUBSCRIPTION_TOTAL("axon.GlobalSubscriptionMetricRegistry.total",
                                   "Total number of subscription queries subscribed"),
    @Deprecated AXON_GLOBAL_SUBSCRIPTION_UPDATES("axon.GlobalSubscriptionMetricRegistry.updates",
                                     "Total number of updates submitted on subscription queries"),
    @Deprecated AXON_GLOBAL_SUBSCRIPTION_ACTIVE("axon.GlobalSubscriptionMetricRegistry.active",
                                    "Active number of subscription queries subscribed"),
    @Deprecated AXON_QUERY_SUBSCRIPTION_TOTAL("axon.QuerySubscriptionMetricRegistry.total",
                                  "Total number of subscription queries subscribed on this node"),
    @Deprecated AXON_QUERY_SUBSCRIPTION_UPDATES("axon.QuerySubscriptionMetricRegistry.updates",
                                    "Total number of updates submitted on subscription queries on this node"),
    @Deprecated AXON_QUERY_SUBSCRIPTION_ACTIVE("axon.QuerySubscriptionMetricRegistry.active",
                                   "Active number of subscription queries on this node"),
    @Deprecated AXON_APPLICATION_SUBSCRIPTION_TOTAL("axon.ApplicationSubscriptionMetricRegistry.total",
                                        "Total number of subscription queries subscribed per application"),
    @Deprecated AXON_APPLICATION_SUBSCRIPTION_UPDATES("axon.ApplicationSubscriptionMetricRegistry.updates",
                                          "Total number of updates submitted on subscription queries per application"),
    @Deprecated AXON_APPLICATION_SUBSCRIPTION_ACTIVE("axon.ApplicationSubscriptionMetricRegistry.active",
                                         "Active number of subscription queries on this node per application"),
    @Deprecated AXON_SUBSCRIPTION_TOTAL("axon.queries.subscriptionquery.throughput.total",
                            "Total number of subscription queries subscribed"),
    @Deprecated AXON_SUBSCRIPTION_DURATION("axon.queries.subscriptionquery.duration",
                               "Duration of a subscription query connection"),
    @Deprecated AXON_SUBSCRIPTION_UPDATES("axon.queries.subscriptionquery.throughput.updates",
                              "Total number of updates submitted on subscription queries"),
    @Deprecated AXON_SUBSCRIPTION_ACTIVE("axon.queries.subscriptionquery.saturation.active",
                             "Active number of subscription queries on this node"),
    @Deprecated AXON_APPLICATION_COMMAND_QUEUE_SIZE("axon.ApplicationCommandQueue.size",
                                        "The size of queue holding commands waiting for permits from client"),
    @Deprecated AXON_APPLICATION_QUERY_QUEUE_SIZE("axon.ApplicationQueryQueue.size",
                                      "The size of queue holding queries waiting for permits from client"),
    AXON_INDEX_OPEN("file.index.open", "Rate of index files opened"),
    AXON_INDEX_CLOSE("file.index.close", "Rate of index files closed"),
    AXON_BLOOM_OPEN("file.bloom.open", "Number of bloom filter files opened"),
    AXON_BLOOM_CLOSE("file.bloom.close", "Number of bloom filter files closed"),
    AXON_SEGMENT_OPEN("file.segment.open", "Number of event store segment files opened"),
    AXON_SEGMENT_MOVE_INITIATED("file.segment.moved.initiated",
                                "Number of event store segment files initiated to be moved to next tier"),
    AXON_SEGMENT_MOVED("file.segment.moved.completed", "Number of event store segment files moved to next tier"),
    AXON_SEGMENT_MOVED_DURATION("file.segment.moved.duration",
                                "Duration of moving event store segment files to next tier"),
    AXON_SEGMENTS_PER_TIER("file.segment.per.tier", "Current number of segments in a tier"),
    AXON_AGGREGATE_READTIME("local.aggregate.readtime", "Elapsed time for reading events from the event store"),
    AXON_AGGREGATE_SEGMENT_COUNT("local.aggregate.segments", "Number of segments where aggregate is found"),
    AXON_LAST_SEQUENCE_READTIME("local.lastsequence.readtime",
                                "Elapsed time for retrieving the last sequence number for an aggregate"),
    @Deprecated LOCAL_QUERY_RESPONSE_TIME("local.query.responsetime",
                              "Response time for query execution from time received by Axon Server to response received"),
    INTERCEPTOR_DURATION("local.interceptor.duration",
                         "Total time executing interceptors (tags: context and interceptor type)");


    private final String name;
    private final String description;

    BaseMetricName(String name,
                   String description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public String metric() {
        return name;
    }

    @Override
    public String description() {
        return description;
    }
}
