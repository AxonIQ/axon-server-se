/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.Metrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

/**
 * @author Marc Gathier
 */
@Service("CommandMetricsRegistry")
public class CommandMetricsRegistry {
    private final Logger logger = LoggerFactory.getLogger(CommandMetricsRegistry.class);

    private final Map<String, Timer> timerMap = new ConcurrentHashMap<>();
    private final MeterFactory meterFactory;

    public CommandMetricsRegistry( MeterFactory meterFactory) {
        this.meterFactory = meterFactory;
    }


    public void add(String command, String sourceClientId, ClientIdentification clientId, long duration) {
        try {
            timer(command, sourceClientId, clientId).record(duration, TimeUnit.MILLISECONDS);
        } catch( Exception ex) {
            logger.debug("Failed to create timer", ex);
        }
    }

    private Timer timer(String command, String sourceClientId, ClientIdentification clientId) {
        return timerMap.computeIfAbsent(metricName(command, sourceClientId, clientId),
                                        n -> meterFactory.timer("axon.command",
                                                                command.replaceAll("\\.", "/"),
                                                                clientId.getContext(),
                                                                sourceClientId,
                                                                clientId.getClient()));
    }

    private static String metricName(String command, String sourceClientId, ClientIdentification clientId) {
        return String.format("%s.%s.%s", command, sourceClientId, clientId.metricName());
    }

    private ClusterMetric clusterMetric(String command, ClientIdentification clientId){
        Tags tags = Tags.of(MeterFactory.CONTEXT,
                            clientId.getContext(),
                            MeterFactory.REQUEST,
                            command.replaceAll("\\.", "/"),
                            MeterFactory.TARGET,
                            clientId.getClient());
        return new CompositeMetric(meterFactory.snapshot("axon.command", tags),
                                   new Metrics("axon.command", tags, meterFactory.clusterMetrics()));
    }

    public CommandMetric commandMetric(String command, ClientIdentification clientId, String componentName) {
        return new CommandMetric(command,clientId.metricName(), componentName, clusterMetric(command, clientId).size());
    }

    public <T> Gauge gauge(String activeCommandsGauge, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return meterFactory.gauge(activeCommandsGauge, objectToWatch, gaugeFunction);
    }

    public MeterFactory.RateMeter rateMeter(String context, String... meterName) {
        return meterFactory.rateMeter(context, meterName);
    }

    public static class CommandMetric {
        private final String command;
        private final String clientId;
        private final String componentName;
        private final long count;

        CommandMetric(String command, String clientId, String componentName, long count) {
            this.command = command;
            this.clientId = clientId;
            this.componentName = componentName;
            this.count = count;
        }

        public String getCommand() {
            return command;
        }

        public String getClientId() {
            return clientId;
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
                    Objects.equals(clientId, that.clientId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(command, clientId);
        }
    }
}
