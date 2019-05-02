/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

import com.google.common.util.concurrent.AtomicDouble;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;

/**
 * Service to create rate based meters.
 * Rate meters are implemented using dropwizard meters, and exposed by defining micrometer gauges.
 * @author Marc Gathier
 * @since 4.2
 */
@Service
public class MeterFactory {

    private final MeterRegistry meterRegistry;
    private final MetricCollector clusterMetrics;
    private final Clock clock = Clock.systemDefaultZone();

    public MeterFactory(MeterRegistry meterRegistry,
                        MetricCollector clusterMetrics) {
        this.meterRegistry = meterRegistry;
        this.clusterMetrics = clusterMetrics;
    }

    public RateMeter rateMeter(String... name) {
        return new RateMeter(String.join(".", name));
    }

    public Counter counter(String name) {
        return Counter.builder(name).register(meterRegistry);
    }

    public Timer timer(String name) {
        return meterRegistry.timer(name);
    }

    public <T> Gauge gauge(String name, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return Gauge.builder(name, objectToWatch, gaugeFunction)
                    .register(meterRegistry);
    }

    public MetricCollector clusterMetrics() {
        return clusterMetrics;
    }

    /**
     * Meter to keep rates of events. These are implemented using an IntervalCounter (that counts number of events in a specific timebucket), and
     * exposed to the actuator/metrics endpoints by {@link Gauge}s.
     */
    public class RateMeter implements Printable {
        private final IntervalCounter meter;

        private final Counter counter;
        private final String name;

        private RateMeter(String name) {
            this.name = name;
            meter = new IntervalCounter(clock);
            counter = meterRegistry.counter(name + ".count");
            meterRegistry.gauge(name + ".oneMinuteRate", meter, IntervalCounter::getOneMinuteRate);
            meterRegistry.gauge(name + ".fiveMinuteRate", meter, IntervalCounter::getFiveMinuteRate);
            meterRegistry.gauge(name + ".fifteenMinuteRate", meter, IntervalCounter::getFifteenMinuteRate);
        }


        public void mark() {
            meter.mark();
            counter.increment();
        }

        public long getCount() {
            AtomicLong count = new AtomicLong(meter.count());
            new Metrics(name + ".count", clusterMetrics).forEach(m -> count.addAndGet(m.size()));
            return count.get();
        }

        public double getOneMinuteRate() {
            AtomicDouble rate = new AtomicDouble(meter.getOneMinuteRate());
            new Metrics(name + ".oneMinuteRate", clusterMetrics).forEach(m -> rate.addAndGet(m.mean()));
            return rate.get();
        }

        public double getFiveMinuteRate() {
            AtomicDouble rate = new AtomicDouble(meter.getFiveMinuteRate());
            new Metrics(name + ".fiveMinuteRate", clusterMetrics).forEach(m -> rate.addAndGet(m.mean()));
            return rate.get();
        }

        public double getFifteenMinuteRate() {
            AtomicDouble rate = new AtomicDouble(meter.getFifteenMinuteRate());
            new Metrics(name + ".fifteenMinuteRate", clusterMetrics).forEach(m -> rate.addAndGet(m.mean()));
            return rate.get();
        }

        public String getName() {
            return name;
        }

        @Override
        public void printOn(Media media) {
            media.with("count", getCount());
            media.with("oneMinuteRate", getOneMinuteRate());
            media.with("fiveMinuteRate", getFiveMinuteRate());
            media.with("fifteenMinuteRate", getFifteenMinuteRate());
        }
    }
}
