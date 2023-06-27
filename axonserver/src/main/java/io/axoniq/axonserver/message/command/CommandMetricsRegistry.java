/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
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
import io.axoniq.axonserver.metric.Metrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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

    private final Map<String, Timer> timerMap = new ConcurrentHashMap<>();
    private final MeterFactory meterFactory;

    /**
     * Constructor for the registy.
     *
     * @param meterFactory the factory to create meter objects
     */
    public CommandMetricsRegistry(MeterFactory meterFactory) {
        this.meterFactory = meterFactory;
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        meterFactory.remove(BaseMetricName.AXON_COMMAND, MeterFactory.SOURCE, event.getClientId());
        meterFactory.remove(BaseMetricName.AXON_COMMAND, MeterFactory.TARGET, event.getClientId());
    }


    private static String metricName(String command,
                                     String sourceClientId,
                                     String targetClientId,
                                     String context) {
        return String.join(".", command, sourceClientId, targetClientId, context);
    }

    /**
     * Registers the duration of a command in the registry. Timer name is "axon.command", tags are the context,
     * the source, the target and the command name.
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
            timer(command, sourceClientId, targetClientId, context).record(duration, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            logger.debug("Failed to create timer", ex);
        }
    }

    private Timer timer(String command,
                        String sourceClientId,
                        String targetClientId,
                        String context) {
        return timerMap.computeIfAbsent(metricName(command, sourceClientId, targetClientId, context),
                                        n -> meterFactory.timer(BaseMetricName.AXON_COMMAND,
                                                                Tags.of(
                                                                        MeterFactory.REQUEST,
                                                                        command.replaceAll("\\.", "/"),
                                                                        MeterFactory.CONTEXT,
                                                                        context,
                                                                        MeterFactory.SOURCE,
                                                                        sourceClientId,
                                                                        MeterFactory.TARGET,
                                                                        targetClientId)));
    }


    private ClusterMetric clusterMetric(String command,
                                        String targetClientId,
                                        String context) {
        Tags tags = Tags.of(MeterFactory.CONTEXT,
                            context,
                            MeterFactory.REQUEST,
                            command.replaceAll("\\.", "/"),
                            MeterFactory.TARGET,
                            targetClientId);
        return new CompositeMetric(meterFactory.snapshot(BaseMetricName.AXON_COMMAND, tags),
                                   new Metrics(BaseMetricName.AXON_COMMAND.metric(),
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
    public <T> Gauge gauge(BaseMetricName name, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return meterFactory.gauge(name, objectToWatch, gaugeFunction);
    }

    /**
     * Creates a meter that will monitor the rate of certain events. The RateMeter will expose events/second for the
     * last 1/5/15 minutes and a
     * total count. The meter will have the context as a tag.
     *
     * @param context   the name of the context
     * @param meterName the name of the meter
     * @return a RateMeter object
     */
    public MeterFactory.RateMeter rateMeter(String context, BaseMetricName meterName) {
        return meterFactory.rateMeter(meterName, Tags.of(MeterFactory.CONTEXT, context));
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
                    Objects.equals(clientId, that.clientId)&&
                    Objects.equals(context, that.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(command, clientId, context);
        }
    }
}
