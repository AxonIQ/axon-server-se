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
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
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

    public static final String CONTEXT = "context";
    public static final String REQUEST = "request";
    public static final String SOURCE = "source";
    public static final String TARGET = "target";

    private final MeterRegistry meterRegistry;
    private final MetricCollector clusterMetrics;
    private final Clock clock = Clock.systemDefaultZone();

    public MeterFactory(MeterRegistry meterRegistry,
                        MetricCollector clusterMetrics) {
        this.meterRegistry = meterRegistry;
        this.clusterMetrics = clusterMetrics;
    }

    public RateMeter rateMeter(String context, String... name) {
        return new RateMeter(context, String.join(".", name));
    }

    public Timer timer(String name, String request, String context, String source, String target) {
        return meterRegistry.timer(name, REQUEST, request, CONTEXT, context, SOURCE, source, TARGET, target);
    }

    public <T> Gauge gauge(String name, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return Gauge.builder(name, objectToWatch, gaugeFunction)
                    .register(meterRegistry);
    }

    public MetricCollector clusterMetrics() {
        return clusterMetrics;
    }

    public SnapshotMetric snapshot(String metric, Tags tags) {
        long count = 0;
        long max = 0;
        double total = 0;

        for (Timer timer : meterRegistry.find(metric).tags(tags).timers()) {
            HistogramSnapshot snapshot = timer.takeSnapshot();
            if (snapshot != null) {
                total += snapshot.total();
                count += snapshot.count();
                max = Math.max(max, (long) snapshot.max());
            }
        }

        return new SnapshotMetric(count, max, count == 0 ? 0 : total / count);
    }

    /**
     * Meter to keep rates of events. These are implemented using an IntervalCounter (that counts number of events in a specific timebucket), and
     * exposed to the actuator/metrics endpoints by {@link Gauge}s.
     */
    public class RateMeter implements Printable {
        private final IntervalCounter meter;

        private final Counter counter;
        private final String name;
        private final Tags tags;

        private RateMeter(String context, String name) {
            this.name = name;
            this.tags = Tags.of(CONTEXT, context);
            meter = new IntervalCounter(clock);
            counter = meterRegistry.counter(name + ".count", CONTEXT, context);
            meterRegistry.gauge(name + ".oneMinuteRate",
                                tags,
                                meter,
                                IntervalCounter::getOneMinuteRate);
            meterRegistry.gauge(name + ".fiveMinuteRate",
                                tags,
                                meter,
                                IntervalCounter::getFiveMinuteRate);
            meterRegistry.gauge(name + ".fifteenMinuteRate",
                                tags,
                                meter,
                                IntervalCounter::getFifteenMinuteRate);
        }


        public void mark() {
            meter.mark();
            counter.increment();
        }

        public long getCount() {
            AtomicLong count = new AtomicLong(meter.count());
            new Metrics(name + ".count", tags, clusterMetrics).forEach(m -> count.addAndGet(m.size()));
            return count.get();
        }

        public double getOneMinuteRate() {
            AtomicDouble rate = new AtomicDouble(meter.getOneMinuteRate());
            new Metrics(name + ".oneMinuteRate", tags, clusterMetrics).forEach(m -> rate.addAndGet(m.mean()));
            return rate.get();
        }

        public double getFiveMinuteRate() {
            AtomicDouble rate = new AtomicDouble(meter.getFiveMinuteRate());
            new Metrics(name + ".fiveMinuteRate", tags, clusterMetrics).forEach(m -> rate.addAndGet(m.mean()));
            return rate.get();
        }

        public double getFifteenMinuteRate() {
            AtomicDouble rate = new AtomicDouble(meter.getFifteenMinuteRate());
            new Metrics(name + ".fifteenMinuteRate", tags, clusterMetrics).forEach(m -> rate.addAndGet(m.mean()));
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
