/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandMetricsRegistryTest {

    private CommandMetricsRegistry testSubject;

    @Before
    public void setUp() {
        testSubject = new CommandMetricsRegistry(new MeterFactory(new SimpleMeterRegistry(),
                                                                  new DefaultMetricCollector()), true);
    }

    @Test
    public void add() {
        testSubject.add("Command", "source1", "target1", Topology.DEFAULT_CONTEXT, 1);
        assertEquals(1L, testSubject.commandMetric("Command",  "target1",Topology.DEFAULT_CONTEXT, null).getCount());
    }

    @Test
    public void addAfterReconnect() {
        testSubject.add("Command", "source1", "target1", Topology.DEFAULT_CONTEXT, 1);
        assertEquals(1L, testSubject.commandMetric("Command",  "target1",Topology.DEFAULT_CONTEXT, null).getCount());
        testSubject.on(new TopologyEvents.ApplicationDisconnected(Topology.DEFAULT_CONTEXT, null, "source1", "disconnect" ));
        testSubject.add("Command", "source1", "target1", Topology.DEFAULT_CONTEXT, 1);
        assertEquals(1L, testSubject.commandMetric("Command",  "target1",Topology.DEFAULT_CONTEXT, null).getCount());
    }


    @Test
    public void testRegistryWithLabels() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        Timer timer = meterRegistry.timer("sample", "tag1", "value1", "tag2", "value2");
        timer.record(1, TimeUnit.SECONDS);
        timer = meterRegistry.timer("sample", "tag1", "value2", "tag2", "value2");
        timer.record(1, TimeUnit.SECONDS);


        HistogramSnapshot snapshot = meterRegistry.find("sample")
                                                  .tags("tag2", "value2")
                                                  .timer().takeSnapshot();
        System.out.println(snapshot);
        meterRegistry.find("sample").tags("tag2", "value2").meters().forEach(m -> System.out
                .printf("%s = %s%n", name(m), value(m)));
    }

    private String value(Meter m) {
        if (m instanceof Timer) {
            return String.valueOf(((Timer) m).count());
        }
        if (m instanceof Gauge) {
            return String.valueOf(((Gauge) m).value());
        }
        if (m instanceof Counter) {
            return String.valueOf(((Counter) m).count());
        }
        return m.getClass().getName();
    }

    private String name(Meter m) {
        String tags = m.getId().getTags().stream().map(t -> t.getKey() + "=" + t.getValue()).collect(Collectors.joining(
                ","));
        return m.getId().getName() + "[" + tags + "]";
    }

}
