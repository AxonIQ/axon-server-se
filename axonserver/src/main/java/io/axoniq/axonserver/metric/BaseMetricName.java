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
    AXON_COMMAND("axon.commands"),
    /**
     * Metric for the rate of commands handled (tags: context)
     */
    AXON_COMMAND_RATE("axon.command"),
    /**
     * Metric for the number of currently active commands
     */
    AXON_ACTIVE_COMMANDS("axon.commands.active"),
    /**
     * Metric for number of queries handled by a client (tags: source client/handler client/context/query)
     */
    AXON_QUERY("axon.queries"),
    /**
     * Metric for the rate of queries handled (tags: context)
     */
    AXON_QUERY_RATE("axon.query"),
    /**
     * Metric for the number of currently active queries
     */
    AXON_ACTIVE_QUERIES("axon.queries.active"),
    /**
     * Metric for the token of the last event in the event store (tags: context)
     */
    AXON_LAST_TOKEN("axon.lastToken"),
    /**
     * Metric for the rate of events stored (tags: context)
     */
    AXON_EVENTS("axon.event"),
    /**
     * Metric for the rate of snapshots stored (tags: context)
     */
    AXON_SNAPSHOTS("axon.snapshot"),
    AXON_GLOBAL_SUBSCRIPTION_TOTAL("axon.GlobalSubscriptionMetricRegistry.total"),
    AXON_GLOBAL_SUBSCRIPTION_UPDATES("axon.GlobalSubscriptionMetricRegistry.updates"),
    AXON_GLOBAL_SUBSCRIPTION_ACTIVE("axon.GlobalSubscriptionMetricRegistry.active"),
    AXON_QUERY_SUBSCRIPTION_TOTAL("axon.QuerySubscriptionMetricRegistry.total"),
    AXON_QUERY_SUBSCRIPTION_UPDATES("axon.QuerySubscriptionMetricRegistry.updates"),
    AXON_QUERY_SUBSCRIPTION_ACTIVE("axon.QuerySubscriptionMetricRegistry.active"),
    AXON_APPLICATION_SUBSCRIPTION_TOTAL("axon.ApplicationSubscriptionMetricRegistry.total"),
    AXON_APPLICATION_SUBSCRIPTION_UPDATES("axon.ApplicationSubscriptionMetricRegistry.updates"),
    AXON_APPLICATION_SUBSCRIPTION_ACTIVE("axon.ApplicationSubscriptionMetricRegistry.active");


    private final String name;

    BaseMetricName(String name) {
        this.name = name;
    }

    @Override
    public String metric() {
        return name;
    }
}
