/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

public enum StandardMetricName implements MetricName {
    /**
     * Base name for the metrics related to the command throughput. Creates the metrics axon.command.count,
     * axon.command.rate.oneMinuteRate, axon.command.rate.fiveMinuteRate and axon.command.rate.fifteenMinuteRate.
     */
    COMMAND_THROUGHPUT("axon.commands.throughput", "Number of commands"),
    COMMAND_DURATION("axon.commands.duration",
                     "Duration of commands, from received by Axon Server until response sent to requester"),
    COMMAND_HANDLING_DURATION("axon.commands.duration.handling",
                              "Duration of commands, from sent to handler until response received from handler"),
    COMMAND_ERRORS("axon.commands.error.count", "Number of errors"),
    COMMAND_QUEUED("axon.commands.saturation.queued.count", "Number of commands queued in Axon Server"),
    COMMAND_ACTIVE("axon.command.saturation.active.count", "Number of in-flight commands"),
    EVENTSTORE_FORCE_LAG("axon.eventstore.saturation.force.lag", "Number of unforced entries"),
    EVENTSTORE_FORCE_DURATION("axon.eventstore.duration.force", "Duration of a force to disk"),
    APPLICATION_CONNECT_DURATION("axon.applications.duration.connection", "Duration of application connections"),
    APPLICATION_CONNECT("axon.applications.saturation.connect.count", "Number of application connect requests"),
    APPLICATION_CONNECTED("axon.applications.saturation.connected.count", "Number of applications currently connected"),
    APPLICATION_DISCONNECT("axon.applications.saturation.disconnect.count",
                           "Number of application disconnect requests"),
    AUTHENTICATION_ERRORS("axon.authentication.errors.count", "Number of authentication errors");

    private final String description;
    private final String metric;

    StandardMetricName(String metric, String description) {
        this.description = description;
        this.metric = metric;
    }

    @Override
    public String metric() {
        return metric;
    }

    @Override
    public String description() {
        return description;
    }
}
