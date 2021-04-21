/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.plugin.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.plugin.interceptor.CommandResponseInterceptor;
import io.axoniq.axonserver.refactoring.messaging.api.Error;
import io.axoniq.axonserver.refactoring.messaging.api.Message;
import io.axoniq.axonserver.refactoring.messaging.api.SerializedObject;
import io.axoniq.axonserver.refactoring.messaging.command.DefaultCommandInterceptors;
import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommand;
import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommandResponse;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandDefinition;
import io.axoniq.axonserver.refactoring.metric.DefaultMetricCollector;
import io.axoniq.axonserver.refactoring.metric.MeterFactory;
import io.axoniq.axonserver.refactoring.plugin.PluginContextFilter;
import io.axoniq.axonserver.refactoring.plugin.PluginEnabledEvent;
import io.axoniq.axonserver.refactoring.plugin.PluginKey;
import io.axoniq.axonserver.refactoring.plugin.ServiceWithInfo;
import io.axoniq.axonserver.refactoring.transport.Mapper;
import io.axoniq.axonserver.refactoring.transport.grpc.CommandMapper;
import io.axoniq.axonserver.refactoring.transport.grpc.CommandResponseMapper;
import io.axoniq.axonserver.refactoring.transport.grpc.MetadataMapper;
import io.axoniq.axonserver.refactoring.transport.grpc.SerializedObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import java.time.Instant;
import java.util.Optional;

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

    private Mapper<SerializedObject, io.axoniq.axonserver.grpc.SerializedObject> serializedObjectMapper = new SerializedObjectMapper();
    private final DefaultCommandInterceptors testSubject = new DefaultCommandInterceptors(pluginContextFilter,
                                                                                          new CommandMapper(),
                                                                                          new CommandResponseMapper(
                                                                                                  serializedObjectMapper,
                                                                                                  new MetadataMapper(
                                                                                                          serializedObjectMapper)),
                                                                                          meterFactory);

    @Test
    public void commandRequest() {
        osgiController.add(new ServiceWithInfo<>((CommandRequestInterceptor) (command, executionContext) ->
                Command.newBuilder()
                       .putMetaData("demo", metaDataValue("demoValue")).build(),
                                                 PLUGIN_KEY));

//        Command intercepted = testSubject.commandRequest(serializedCommand("sample"),
//                                                                   new TestExecutionContext("default"));
//        assertFalse(intercepted.wrapped().containsMetaData("demo"));
//
//        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
//        intercepted = testSubject.commandRequest(serializedCommand("sample"), new TestExecutionContext("default"));
//        assertTrue(intercepted.wrapped().containsMetaData("demo"));
    }

    private io.axoniq.axonserver.refactoring.messaging.command.api.Command serializedCommand(String sample) {
        return new io.axoniq.axonserver.refactoring.messaging.command.api.Command() {
            @Override
            public CommandDefinition definition() {
                return new CommandDefinition() {
                    @Override
                    public String name() {
                        return sample;
                    }

                    @Override
                    public String context() {
                        return null;
                    }
                };
            }

            @Override
            public Message message() {
                return null;
            }

            @Override
            public String routingKey() {
                return null;
            }

            @Override
            public Instant timestamp() {
                return null;
            }
        };
    }


    @Test
    public void commandResponse() {
        osgiController.add(new ServiceWithInfo<>((CommandResponseInterceptor) (commandResponse, executionContext) ->
                CommandResponse.newBuilder()
                               .putMetaData("demo", metaDataValue("demoValue")).build(),
                                                 PLUGIN_KEY));

//        SerializedCommandResponse intercepted = testSubject.commandResponse(serializedCommandResponse("test"),
//                                                                            new TestExecutionContext("default"));
//        assertFalse(intercepted.wrapped().containsMetaData("demo"));
//
//        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
//        intercepted = testSubject.commandResponse(serializedCommandResponse("sample"),
//                                                  new TestExecutionContext("default"));
//        assertTrue(intercepted.wrapped().containsMetaData("demo"));
    }

    private io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse serializedCommandResponse(
            String test) {
        return new io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse() {
            @Override
            public String requestId() {
                return test;
            }

            @Override
            public Message message() {
                return null;
            }

            @Override
            public Optional<Error> error() {
                return Optional.empty();
            }
        };
    }

    private MetaDataValue metaDataValue(String demoValue) {
        return MetaDataValue.newBuilder().setTextValue(demoValue).build();
    }
}