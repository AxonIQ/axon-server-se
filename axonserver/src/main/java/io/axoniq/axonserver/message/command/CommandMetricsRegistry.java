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
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricName;
import io.axoniq.axonserver.metric.Metrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

/**
 * Provides access to metrics regarding command execution.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@Service("CommandMetricsRegistry")
public class CommandMetricsRegistry {

    private final Logger logger = LoggerFactory.getLogger(CommandMetricsRegistry.class);

    private final MeterFactory meterFactory;
    private final boolean legacyMetricsEnabled;

    /**
     * Constructor for the registy.
     *
     * @param meterFactory the factory to create meter objects
     */
    public CommandMetricsRegistry(MeterFactory meterFactory,
                                  @Value("${axoniq.axonserver.legacy-metrics-enabled:true}") boolean legacyMetricsEnabled) {
        this.meterFactory = meterFactory;
        this.legacyMetricsEnabled = legacyMetricsEnabled;
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        meterFactory.remove(BaseMetricName.COMMAND_DURATION, MeterFactory.SOURCE, event.getClientId());
        meterFactory.remove(BaseMetricName.COMMAND_DURATION, MeterFactory.TARGET, event.getClientId());
        if (legacyMetricsEnabled) {
            meterFactory.remove(BaseMetricName.AXON_COMMAND, MeterFactory.SOURCE, event.getClientId());
            meterFactory.remove(BaseMetricName.AXON_COMMAND, MeterFactory.TARGET, event.getClientId());
        }
    }

    public void remove(MetricName metricName, String context) {
        meterFactory.remove(metricName, MeterFactory.CONTEXT, context);
    }

    /**
     * Registers the duration of a command in the registry. Timer name is "axon.command", tags are the context, the
     * source, the target and the command name.
     *
     * @param command        the name of the command
     * @param sourceClientId the client issuing the command
     * @param targetClientId the client handling the command
     * @param context        the principal context of the client handling the command
     * @param duration       the duration of the command handling
     */
    public void add(String command,
                    String sourceClientId,
                    String targetClientId,
                    String context,
                    long duration) {
        try {
            if (legacyMetricsEnabled) {
                legacyTimer(command, sourceClientId, targetClientId, context).record(duration, TimeUnit.MILLISECONDS);
            }
            timer(command, sourceClientId, targetClientId, context).record(duration, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            logger.debug("Failed to create timer", ex);
        }
    }

    private Timer timer(String command,
                        String sourceClientId,
                        String targetClientId,
                        String context) {
        return meterFactory.timer(BaseMetricName.COMMAND_DURATION,
                                  Tags.of(
                                          MeterFactory.REQUEST,
                                          command,
                                          MeterFactory.CONTEXT,
                                          context,
                                          MeterFactory.SOURCE,
                                          sourceClientId,
                                          MeterFactory.TARGET,
                                          targetClientId));
    }

    private Timer legacyTimer(String command,
                              String sourceClientId,
                              String targetClientId,
                              String context) {
        return meterFactory.timer(BaseMetricName.AXON_COMMAND,
                                                                Tags.of(
                                                                        MeterFactory.REQUEST,
                                                                        normalizeCommandName(command),
                                                                        MeterFactory.CONTEXT,
                                                                        context,
                                                                        MeterFactory.SOURCE,
                                                                        sourceClientId,
                                                                        MeterFactory.TARGET,
                                                                        targetClientId));
    }

    private String normalizeCommandName(String command) {
        return command.replace(".", "/");
    }


    private ClusterMetric clusterMetric(String command,
                                        String targetClientId,
                                        String context) {
        Tags tags = Tags.of(MeterFactory.CONTEXT,
                            context,
                            MeterFactory.REQUEST,
                            command,
                            MeterFactory.TARGET,
                            targetClientId);
        return new CompositeMetric(meterFactory.snapshot(BaseMetricName.COMMAND_DURATION, tags),
                                   new Metrics(BaseMetricName.COMMAND_DURATION.metric(),
                                               tags,
                                               meterFactory.clusterMetrics()));
    }

    /**
     * Retrieves the number of times that a command has been handled by a client.
     *
     * @param command        the name of the command
     * @param targetClientId the client handling the command
     * @param context        the principal context of the client handling the command
     * @param componentName  the client application name handling the command
     * @return CommandMetric containing the number of times that the command has been handled by this client
     */
    public CommandMetric commandMetric(String command,
                                       String targetClientId,
                                       String context,
                                       String componentName) {
        return new CommandMetric(command,
                                 targetClientId,
                                 context,
                                 componentName,
                                 clusterMetric(command, targetClientId, context).count());
    }

    /**
     * Creates a gauge meter without any tags
     *
     * @param name          the name of the gauge
     * @param objectToWatch the object to watch
     * @param gaugeFunction function that will be applied on the object to retrieve the gauge value
     * @param <T>           type of object to watch
     * @return a gauge object
     */
    public <T> Gauge gauge(MetricName name, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return meterFactory.gauge(name, objectToWatch, gaugeFunction);
    }

    public Gauge gauge(MetricName name, Tags tags, Supplier<Number> gaugeFunction) {
        return meterFactory.gauge(name, tags, gaugeFunction);
    }

    /**
     * Creates a meter that will monitor the rate of certain events. The RateMeter will expose events/second for the
     * last 1/5/15 minutes and a total count. The meter will have the context as a tag.
     *
     * @param context   the name of the context
     * @param meterName the name of the meter
     * @return a RateMeter object
     */
    public MeterFactory.RateMeter rateMeter(String context, MetricName meterName, MetricName legacyMetricName) {
        return meterFactory.rateMeter(meterName,
                                      legacyMetricsEnabled ? legacyMetricName : null,
                                      Tags.of(MeterFactory.CONTEXT, context));
    }

    public void error(String command, String context, String errorCode) {
        meterFactory.counter(BaseMetricName.COMMAND_ERRORS, Tags.of(MeterFactory.CONTEXT,
                                                                        context,
                                                                        MeterFactory.REQUEST,
                                                                        command,
                                                                        MeterFactory.ERROR_CODE,
                                                                        errorCode)).increment();
    }

    public void removeForContext(String context) {
        meterFactory.remove(BaseMetricName.COMMAND_DURATION, MeterFactory.CONTEXT, context);
        meterFactory.remove(BaseMetricName.COMMAND_HANDLING_DURATION, MeterFactory.CONTEXT, context);
        meterFactory.remove(BaseMetricName.COMMAND_ERRORS, MeterFactory.CONTEXT, context);
    }

    public static class CommandMetric {

        private final String command;
        private final String clientId;

        private final String context;
        private final String componentName;
        private final long count;

        CommandMetric(String command, String clientId, String context, String componentName, long count) {
            this.command = command;
            this.clientId = clientId;
            this.context = context;
            this.componentName = componentName;
            this.count = count;
        }

        public String getCommand() {
            return command;
        }

        public String getClientId() {
            return clientId;
        }

        public String getContext() {
            return context;
        }

        public String getComponentName() {
            return componentName;
        }

        public long getCount() {
            return count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CommandMetric that = (CommandMetric) o;
            return Objects.equals(command, that.command) &&
                    Objects.equals(clientId, that.clientId) &&
                    Objects.equals(context, that.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(command, clientId, context);
        }
    }
}
