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
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;

/**
 * Service to create rate based meters. Rate meters are implemented using dropwizard meters, and exposed by defining
 * micrometer gauges.
 *
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

    public RateMeter rateMeter(MetricName metric, Tags tags) {
        return new RateMeter(metric, tags);
    }

    public Counter counter(MetricName metric, Tags tags) {
        return Counter.builder(metric.metric())
                      .tags(tags)
                      .description(metric.description())
                      .register(meterRegistry);
    }

    public Counter counter(MetricName metric) {
        return counter(metric, Tags.empty());
    }

    public Timer timer(MetricName metric, Tags tags) {
        return Timer.builder(metric.metric())
                    .tags(tags)
                    .description(metric.description())
                    .register(meterRegistry);
    }

    public DistributionSummary distributionSummary(MetricName metric, Tags tags) {
        return DistributionSummary.builder(metric.metric())
                                  .description(metric.description())
                                  .baseUnit("count")
                                  .publishPercentiles(0.5, 0.9, 0.95, 0.99)
                                  .tags(tags)
                                  .register(meterRegistry);
    }

    public <T> Gauge gauge(MetricName metric, Tags tags, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return Gauge.builder(metric.metric(), objectToWatch, gaugeFunction)
                    .tags(tags)
                    .description(metric.description())
                    .register(meterRegistry);
    }

    public <T> Gauge gauge(MetricName metric, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return Gauge.builder(metric.metric(), objectToWatch, gaugeFunction)
                    .description(metric.description())
                    .register(meterRegistry);
    }

    public MetricCollector clusterMetrics() {
        return clusterMetrics;
    }

    public SnapshotMetric snapshot(MetricName metric, Tags tags) {
        long count = 0;
        double max = 0;
        double total = 0;

        for (Timer timer : meterRegistry.find(metric.metric()).tags(tags).timers()) {
            HistogramSnapshot snapshot = timer.takeSnapshot();
            if (snapshot != null) {
                total += snapshot.total();
                count += snapshot.count();
                max = Math.max(max, snapshot.max());
            }
        }

        return new SnapshotMetric(max, count == 0 ? 0 : total / count, count);
    }

    public void remove(Meter meter) {
        meterRegistry.remove(meter);
    }

    public void remove(BaseMetricName axonQuery, String tagName, String tagValue) {
        List<Meter> toDelete = new LinkedList<>();
        meterRegistry.find(axonQuery.metric()).meters().forEach(m -> {
            if (tagValue.equals(m.getId().getTag(tagName))) {
               toDelete.add(m);
            }
        });
        toDelete.forEach(meterRegistry::remove);
    }

    /**
     * Meter to keep rates of events. These are implemented using an IntervalCounter (that counts number of events in a
     * specific timebucket), and exposed to the actuator/metrics endpoints by {@link Gauge}s.
     */
    public class RateMeter implements Printable {

        public static final String ONE_MINUTE_RATE = ".rate.oneMinuteRate";
        public static final String FIVE_MINUTE_RATE = ".rate.fiveMinuteRate";
        public static final String FIFTEEN_MINUTE_RATE = ".rate.fifteenMinuteRate";
        public static final String COUNT = ".count";
        private final IntervalCounter meter;

        private final Counter deprecatedCounter;
        private final Counter counter;
        private final String name;
        private final Tags tags;

        private RateMeter(MetricName metricName, Tags tags) {
            this.name = metricName.metric();
            this.tags = tags;
            meter = new IntervalCounter(clock);
            counter = Counter.builder(name + ".count")
                             .description(metricName.description() + " since start")
                             .tags(tags)
                             .register(meterRegistry);

            deprecatedCounter =
                    Counter.builder(name + ".rate.count")
                           .description(metricName.description() + " since start [Deprecated]")
                           .tags(tags)
                           .register(meterRegistry);

            Gauge.builder(name + ONE_MINUTE_RATE, meter, IntervalCounter::getOneMinuteRate)
                 .description(metricName.description() + " per second (last minute)")
                 .tags(tags)
                 .register(meterRegistry);
            Gauge.builder(name + FIVE_MINUTE_RATE, meter, IntervalCounter::getFiveMinuteRate)
                 .description(metricName.description() + " per second (last 5 minutes)")
                 .tags(tags)
                 .register(meterRegistry);
            Gauge.builder(name + FIFTEEN_MINUTE_RATE, meter, IntervalCounter::getFifteenMinuteRate)
                 .description(metricName.description() + " per second (last 15 minutes)")
                 .tags(tags)
                 .register(meterRegistry);
        }


        public void mark() {
            meter.mark();
            counter.increment();
            deprecatedCounter.increment();
        }

        public long getCount() {
            AtomicLong count = new AtomicLong(meter.count());
            new Metrics(name + COUNT, tags, clusterMetrics).forEach(m -> count.addAndGet(m.count()));
            return count.get();
        }

        public double getOneMinuteRate() {
            AtomicDouble rate = new AtomicDouble(meter.getOneMinuteRate());
            new Metrics(name + ONE_MINUTE_RATE, tags, clusterMetrics).forEach(m -> rate.addAndGet(m.value()));
            return rate.get();
        }

        public double getFiveMinuteRate() {
            AtomicDouble rate = new AtomicDouble(meter.getFiveMinuteRate());
            new Metrics(name + FIVE_MINUTE_RATE, tags, clusterMetrics).forEach(m -> rate.addAndGet(m.value()));
            return rate.get();
        }

        public double getFifteenMinuteRate() {
            AtomicDouble rate = new AtomicDouble(meter.getFifteenMinuteRate());
            new Metrics(name + FIFTEEN_MINUTE_RATE, tags, clusterMetrics).forEach(m -> rate
                    .addAndGet(m.value()));
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
