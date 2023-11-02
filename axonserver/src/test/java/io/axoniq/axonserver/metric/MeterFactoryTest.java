/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

import static io.axoniq.axonserver.metric.MeterFactory.RateMeter.COUNT;
import static io.axoniq.axonserver.metric.MeterFactory.RateMeter.FIFTEEN_MINUTE_RATE;
import static io.axoniq.axonserver.metric.MeterFactory.RateMeter.FIVE_MINUTE_RATE;
import static io.axoniq.axonserver.metric.MeterFactory.RateMeter.ONE_MINUTE_RATE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MeterFactoryTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final MeterFactory testSubject = new MeterFactory(meterRegistry, new DefaultMetricCollector());

    @Test
    public void rateMeterRemoveWithLegacy() {
        MeterFactory.RateMeter meter = testSubject.rateMeter(BaseMetricName.QUERY_THROUGHPUT,
                                                             BaseMetricName.AXON_QUERY_RATE,
                                                             Tags.of("CONTEXT", "default"));
        assertFalse(meterRegistry.find(BaseMetricName.QUERY_THROUGHPUT.metric() + COUNT).meters().isEmpty());
        assertFalse(meterRegistry.find(BaseMetricName.AXON_QUERY_RATE.metric() + COUNT).meters().isEmpty());
        meter.remove();
        assertTrue(meterRegistry.find(BaseMetricName.QUERY_THROUGHPUT.metric() + COUNT).meters().isEmpty());
        assertTrue(meterRegistry.find(BaseMetricName.QUERY_THROUGHPUT.metric() + ONE_MINUTE_RATE).meters().isEmpty());
        assertTrue(meterRegistry.find(BaseMetricName.QUERY_THROUGHPUT.metric() + FIVE_MINUTE_RATE).meters().isEmpty());
        assertTrue(meterRegistry.find(BaseMetricName.QUERY_THROUGHPUT.metric() + FIFTEEN_MINUTE_RATE).meters()
                                .isEmpty());
        assertTrue(meterRegistry.find(BaseMetricName.AXON_QUERY_RATE.metric() + COUNT).meters().isEmpty());
        assertTrue(meterRegistry.find(BaseMetricName.AXON_QUERY_RATE.metric() + ONE_MINUTE_RATE).meters().isEmpty());
        assertTrue(meterRegistry.find(BaseMetricName.AXON_QUERY_RATE.metric() + FIVE_MINUTE_RATE).meters().isEmpty());
        assertTrue(meterRegistry.find(BaseMetricName.AXON_QUERY_RATE.metric() + FIFTEEN_MINUTE_RATE).meters()
                                .isEmpty());
    }

    @Test
    public void rateMeterRemoveWithoutLegacy() {
        MeterFactory.RateMeter meter = testSubject.rateMeter(BaseMetricName.QUERY_THROUGHPUT,
                                                             null,
                                                             Tags.of("CONTEXT", "default"));
        meter.remove();
        assertTrue(meterRegistry.find(BaseMetricName.QUERY_THROUGHPUT.metric() + ".count").meters().isEmpty());
        assertTrue(meterRegistry.find(BaseMetricName.QUERY_THROUGHPUT.metric() + ONE_MINUTE_RATE).meters().isEmpty());
        assertTrue(meterRegistry.find(BaseMetricName.AXON_QUERY_RATE.metric() + ".count").meters().isEmpty());
    }
}