/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.plugin.PluginKey;
import io.axoniq.axonserver.plugin.ServiceWithInfo;
import io.axoniq.axonserver.plugin.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.plugin.interceptor.CommandResponseInterceptor;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class DefaultCommandInterceptorsTest {

    public static final PluginKey PLUGIN_KEY = new PluginKey("sample", "1.0");
    private final TestPluginServiceProvider osgiController = new TestPluginServiceProvider();
    private final PluginContextFilter pluginContextFilter = new PluginContextFilter(osgiController, true);

    private final MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(),
                                                               new DefaultMetricCollector());

    private final DefaultCommandInterceptors testSubject = new DefaultCommandInterceptors(pluginContextFilter,
                                                                                          meterFactory);

    @Test
    public void commandRequest() {
        osgiController.add(new ServiceWithInfo<>((CommandRequestInterceptor) (command, executionContext) ->
                Command.newBuilder()
                       .putMetaData("demo", metaDataValue("demoValue")).build(),
                                                 PLUGIN_KEY));

        SerializedCommand intercepted = testSubject.commandRequest(serializedCommand("sample"),
                                                                   new TestExecutionContext("default"));
        assertFalse(intercepted.wrapped().containsMetaData("demo"));

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        intercepted = testSubject.commandRequest(serializedCommand("sample"), new TestExecutionContext("default"));
        assertTrue(intercepted.wrapped().containsMetaData("demo"));
    }

    private SerializedCommand serializedCommand(String sample) {
        return new SerializedCommand(Command.newBuilder().setName(sample).build());
    }


    @Test
    public void commandResponse() {
        osgiController.add(new ServiceWithInfo<>((CommandResponseInterceptor) (commandResponse, executionContext) ->
                CommandResponse.newBuilder()
                               .putMetaData("demo", metaDataValue("demoValue")).build(),
                                                 PLUGIN_KEY));

        SerializedCommandResponse intercepted = testSubject.commandResponse(serializedCommandResponse("test"),
                                                                            new TestExecutionContext("default"));
        assertFalse(intercepted.wrapped().containsMetaData("demo"));

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        intercepted = testSubject.commandResponse(serializedCommandResponse("sample"),
                                                  new TestExecutionContext("default"));
        assertTrue(intercepted.wrapped().containsMetaData("demo"));
    }

    private SerializedCommandResponse serializedCommandResponse(String test) {
        return new SerializedCommandResponse(CommandResponse.newBuilder().setMessageIdentifier(test).build());
    }

    private MetaDataValue metaDataValue(String demoValue) {
        return MetaDataValue.newBuilder().setTextValue(demoValue).build();
    }
}