/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
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

    /**
     * Metric for number of commands handled by a client (tags: source client/handler client/context/command)
     */
    AXON_COMMAND("axon.commands", "Number of commands handled by the Axon Server node"),
    /**
     * Metric for the rate of commands handled (tags: context)
     */
    AXON_COMMAND_RATE("axon.command", "Number of commands"),
    /**
     * Metric for the number of currently active commands
     */
    AXON_ACTIVE_COMMANDS("axon.commands.active", "Number of pending commands"),
    /**
     * Metric for number of queries handled by a client (tags: source client/handler client/context/query)
     */
    AXON_QUERY("axon.queries", "Number of queries handled by the Axon Server node"),
    /**
     * Metric for the rate of queries handled (tags: context)
     */
    AXON_QUERY_RATE("axon.query", "Number of queries"),
    /**
     * Metric for the number of currently active queries
     */
    AXON_ACTIVE_QUERIES("axon.queries.active", "Number of pending queries"),
    /**
     * Metric for the token of the last event in the event store (tags: context)
     */
    AXON_LAST_TOKEN("axon.lastToken", "Last token in the event store"),
    /**
     * Metric for the rate of events stored (tags: context)
     */
    AXON_EVENTS("axon.event", "Number of event stored per second"),
    /**
     * Metric for the rate of snapshots stored (tags: context)
     */
    AXON_SNAPSHOTS("axon.snapshot", "Number of snapshots stored"),
    AXON_GLOBAL_SUBSCRIPTION_TOTAL("axon.GlobalSubscriptionMetricRegistry.total",
                                   "Total number of subscription queries subscribed"),
    AXON_GLOBAL_SUBSCRIPTION_UPDATES("axon.GlobalSubscriptionMetricRegistry.updates",
                                     "Total number of updates submitted on subscription queries"),
    AXON_GLOBAL_SUBSCRIPTION_ACTIVE("axon.GlobalSubscriptionMetricRegistry.active",
                                    "Active number of subscription queries subscribed"),
    AXON_QUERY_SUBSCRIPTION_TOTAL("axon.QuerySubscriptionMetricRegistry.total",
                                  "Total number of subscription queries subscribed on this node"),
    AXON_QUERY_SUBSCRIPTION_UPDATES("axon.QuerySubscriptionMetricRegistry.updates",
                                    "Total number of updates submitted on subscription queries on this node"),
    AXON_QUERY_SUBSCRIPTION_ACTIVE("axon.QuerySubscriptionMetricRegistry.active",
                                   "Active number of subscription queries on this node"),
    AXON_APPLICATION_SUBSCRIPTION_TOTAL("axon.ApplicationSubscriptionMetricRegistry.total",
                                        "Total number of subscription queries subscribed per application"),
    AXON_APPLICATION_SUBSCRIPTION_UPDATES("axon.ApplicationSubscriptionMetricRegistry.updates",
                                          "Total number of updates submitted on subscription queries per application"),
    AXON_APPLICATION_SUBSCRIPTION_ACTIVE("axon.ApplicationSubscriptionMetricRegistry.active",
                                         "Active number of subscription queries on this node per application");

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
