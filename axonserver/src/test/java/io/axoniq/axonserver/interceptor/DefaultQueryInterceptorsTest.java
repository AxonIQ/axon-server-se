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
import io.axoniq.axonserver.plugin.interceptor.QueryRequestInterceptor;
import io.axoniq.axonserver.plugin.interceptor.QueryResponseInterceptor;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class DefaultQueryInterceptorsTest {

    public static final PluginKey PLUGIN_KEY = new PluginKey("sample", "1.0");
    private final TestPluginServiceProvider osgiController = new TestPluginServiceProvider();
    private final PluginContextFilter pluginContextFilter = new PluginContextFilter(osgiController, true);
    private final MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(),
                                                               new DefaultMetricCollector());
    private final DefaultQueryInterceptors testSubject = new DefaultQueryInterceptors(pluginContextFilter,
                                                                                      meterFactory);

    @Test
    public void queryRequest() {
        osgiController.add(new ServiceWithInfo<>((QueryRequestInterceptor) (queryRequest, executionContext) ->
                QueryRequest.newBuilder(queryRequest)
                            .putMetaData("demo", metaDataValue("demoValue")).build(),
                                                 PLUGIN_KEY));

        SerializedQuery intercepted = testSubject.queryRequest(serializedQuery("sample"),
                                                               new TestExecutionContext("default"));
        assertFalse(intercepted.query().containsMetaData("demo"));

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        intercepted = testSubject.queryRequest(serializedQuery("sample"), new TestExecutionContext("default"));
        assertTrue(intercepted.query().containsMetaData("demo"));
    }

    @Test
    public void queryResponse() {
        osgiController.add(new ServiceWithInfo<>((QueryResponseInterceptor) (queryResponse, executionContext) ->
                QueryResponse.newBuilder(queryResponse)
                             .putMetaData("demo", metaDataValue("demoValue")).build(),
                                                 PLUGIN_KEY));

        QueryResponse intercepted = testSubject.queryResponse(queryResponse("test"),
                                                              new TestExecutionContext("default"));
        assertFalse(intercepted.containsMetaData("demo"));

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        intercepted = testSubject.queryResponse(queryResponse("sample"), new TestExecutionContext("default"));
        assertTrue(intercepted.containsMetaData("demo"));
    }

    private SerializedQuery serializedQuery(String sample) {
        return new SerializedQuery("default", QueryRequest.newBuilder().setQuery(sample).build());
    }

    private QueryResponse queryResponse(String test) {
        return QueryResponse.newBuilder().setMessageIdentifier(test).build();
    }

    private MetaDataValue metaDataValue(String demoValue) {
        return MetaDataValue.newBuilder().setTextValue(demoValue).build();
    }
}