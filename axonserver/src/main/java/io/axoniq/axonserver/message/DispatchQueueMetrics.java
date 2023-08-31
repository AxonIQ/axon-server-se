/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message;

import io.axoniq.axonserver.grpc.ClientContext;
import io.axoniq.axonserver.grpc.ClientIdRegistry;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricName;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import static io.axoniq.axonserver.metric.MeterFactory.CONTEXT;
import static io.axoniq.axonserver.metric.MeterFactory.TARGET;

public class DispatchQueueMetrics implements QueueMetrics {

    private final MeterFactory meterFactory;
    private final MetricName metricName;
    private final MetricName deprecatedMetricName;
    private final ClientIdRegistry clientIdRegistry;

    private final Map<String, Gauge> gauges = new ConcurrentHashMap<>();
    private final Map<String, Gauge> deprecatedGauges = new ConcurrentHashMap<>();

    public DispatchQueueMetrics(MeterFactory meterFactory,
                                MetricName metricName,
                                ClientIdRegistry clientIdRegistry) {
        this(meterFactory, metricName, null, clientIdRegistry);
    }

    public DispatchQueueMetrics(MeterFactory meterFactory,
                                MetricName metricName,
                                MetricName deprecatedMetricName,
                                ClientIdRegistry clientIdRegistry) {
        this.meterFactory = meterFactory;
        this.metricName = metricName;
        this.deprecatedMetricName = deprecatedMetricName;
        this.clientIdRegistry = clientIdRegistry;
    }

    @Override
    public void add(String queueName, Queue<?> queue) {
        ClientContext clientContext = clientIdRegistry.clientId(queueName);
        gauges.put(queueName, meterFactory.gauge(metricName,
                                                 Tags.of(CONTEXT, clientContext.context(),
                                                         TARGET, clientContext.clientId()),
                                                 queue,
                                                 Queue::size));
        if (deprecatedMetricName != null) {
            deprecatedGauges.put(queueName, meterFactory.gauge(deprecatedMetricName, Tags.of("destination", queueName),
                                                               queue,
                                                               Queue::size));
        }
    }

    @Override
    public void remove(String queueName) {
        Gauge gauge = gauges.remove(queueName);
        if (gauge != null) {
            meterFactory.remove(gauge);
        }
        gauge = deprecatedGauges.remove(queueName);
        if (gauge != null) {
            meterFactory.remove(gauge);
        }
    }
}
