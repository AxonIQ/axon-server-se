/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.plugin.PluginKey;
import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.plugin.PostCommitHookException;
import io.axoniq.axonserver.plugin.RequestRejectedException;
import io.axoniq.axonserver.plugin.ServiceWithInfo;
import io.axoniq.axonserver.plugin.hook.PostCommitEventsHook;
import io.axoniq.axonserver.plugin.hook.PostCommitSnapshotHook;
import io.axoniq.axonserver.plugin.hook.PreCommitEventsHook;
import io.axoniq.axonserver.plugin.interceptor.AppendEventInterceptor;
import io.axoniq.axonserver.plugin.interceptor.AppendSnapshotInterceptor;
import io.axoniq.axonserver.plugin.interceptor.ReadEventInterceptor;
import io.axoniq.axonserver.plugin.interceptor.ReadSnapshotInterceptor;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

import static io.axoniq.axonserver.util.StringUtils.getOrDefault;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class DefaultEventInterceptorsTest {

    public static final PluginKey PLUGIN_KEY = new PluginKey("sample", "1.0");
    private final TestPluginServiceProvider pluginServiceProvider = new TestPluginServiceProvider();
    private final PluginContextFilter pluginContextFilter = new PluginContextFilter(pluginServiceProvider,
                                                                                    true);
    private final MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(),
                                                               new DefaultMetricCollector());

    private final DefaultEventInterceptors testSubject = new DefaultEventInterceptors(pluginContextFilter,
                                                                                      meterFactory);


    @Test
    public void appendEvent() {
        pluginServiceProvider.add(new ServiceWithInfo<>((AppendEventInterceptor) (event, executionContext) ->
                Event.newBuilder()
                     .setMessageIdentifier(UUID.randomUUID().toString())
                     .setAggregateIdentifier(UUID.randomUUID().toString())
                     .setPayload(serializedObject(null, null, "data2"))
                     .putMetaData("demo", metaDataValue("demoValue")).build(),
                                                        PLUGIN_KEY));


        Event orgEvent = event("aggregate1", 0);

        Event intercepted = testSubject.appendEvent(orgEvent, new TestExecutionContext("default"));
        assertEquals("sampleData", intercepted.getPayload().getData().toStringUtf8());
        assertFalse(intercepted.containsMetaData("demo"));

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        intercepted = testSubject.appendEvent(orgEvent, new TestExecutionContext("default"));

        assertEquals(orgEvent.getAggregateIdentifier(), intercepted.getAggregateIdentifier());
        assertEquals(orgEvent.getMessageIdentifier(), intercepted.getMessageIdentifier());
        assertEquals("data2", intercepted.getPayload().getData().toStringUtf8());
        assertTrue(intercepted.containsMetaData("demo"));
        assertEquals("demoValue",
                     intercepted.getMetaDataOrDefault("demo", MetaDataValue.getDefaultInstance()).getTextValue());
    }

    @Test
    public void eventsPreCommit() throws RequestRejectedException {
        AtomicInteger hookCalled = new AtomicInteger();
        pluginServiceProvider.add(new ServiceWithInfo<>((PreCommitEventsHook) (events, context) ->
                hookCalled.incrementAndGet(), PLUGIN_KEY));

        TestExecutionContext executionContext = new TestExecutionContext("default");
        testSubject.eventsPreCommit(asList(event("aggrId1", 0),
                                           event("aggrId1", 1)),
                                    executionContext);
        assertEquals(0, hookCalled.get());

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        testSubject.eventsPreCommit(asList(event("aggrId1", 0),
                                           event("aggrId1", 1)),
                                    executionContext);
        assertEquals(1, hookCalled.get());
    }

    @Test
    public void eventsPreCommitTriesToUpdateEventList() throws RequestRejectedException {
        AtomicInteger hookCalled = new AtomicInteger();
        pluginServiceProvider.add(new ServiceWithInfo<>((PreCommitEventsHook) (events, context) -> {
            events.clear();
            hookCalled.incrementAndGet();
        }, PLUGIN_KEY));

        TestExecutionContext executionContext = new TestExecutionContext("default");
        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        try {
            testSubject.eventsPreCommit(asList(event("aggrId1", 0),
                                               event("aggrId1", 1)),
                                        executionContext);
            fail("pre commit fails when hook tries to change event list");
        } catch (MessagingPlatformException messagingPlatformException) {
        }
    }

    @Test
    public void eventsPostCommit() {
        AtomicInteger hookCalled = new AtomicInteger();
        pluginServiceProvider.add(new ServiceWithInfo<>((PostCommitEventsHook) (events, context) ->
                hookCalled.incrementAndGet(), PLUGIN_KEY));

        TestExecutionContext executionContext = new TestExecutionContext("default");
        testSubject.eventsPostCommit(asList(event("aggrId1", 0),
                                            event("aggrId1", 1)),
                                     executionContext);
        assertEquals(0, hookCalled.get());

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        testSubject.eventsPostCommit(asList(event("aggrId1", 0),
                                            event("aggrId1", 1)),
                                     executionContext);
        assertEquals(1, hookCalled.get());
    }

    @Test
    public void eventsPostCommitWithException() {
        pluginServiceProvider.add(new ServiceWithInfo<>((PostCommitEventsHook) (events, context) -> {
            throw new RuntimeException("Error in post commit hook");
        }, PLUGIN_KEY));

        TestExecutionContext executionContext = new TestExecutionContext("default");
        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        try {
            testSubject.eventsPostCommit(asList(event("aggrId1", 0),
                                                event("aggrId1", 1)),
                                         executionContext);
            fail("Expected PostCommitHookException");
        } catch (PostCommitHookException ex) {
            // expected
        }
    }

    @Test
    public void snapshotPostCommitWithException() {
        pluginServiceProvider.add(new ServiceWithInfo<>((PostCommitSnapshotHook) (events, context) -> {
            throw new RuntimeException("Error in post commit hook");
        }, PLUGIN_KEY));

        TestExecutionContext executionContext = new TestExecutionContext("default");
        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        try {
            testSubject.snapshotPostCommit(event("aggrId1", 0), executionContext);
            fail("Expected PostCommitHookException");
        } catch (PostCommitHookException ex) {
            // expected
        }
    }

    @Test
    public void snapshotPostCommit() {
        AtomicInteger hookCalled = new AtomicInteger();
        pluginServiceProvider.add(new ServiceWithInfo<>((PostCommitSnapshotHook) (events, context) -> hookCalled
                .incrementAndGet(), PLUGIN_KEY));

        TestExecutionContext executionContext = new TestExecutionContext("default");
        testSubject.snapshotPostCommit(event("aggrId1", 0), executionContext);
        assertEquals(0, hookCalled.get());

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        testSubject.snapshotPostCommit(event("aggrId1", 0), executionContext);
        assertEquals(1, hookCalled.get());
    }

    @Test
    public void appendSnapshot() throws RequestRejectedException {
        pluginServiceProvider.add(new ServiceWithInfo<>((AppendSnapshotInterceptor) (event, executionContext) ->
                Event.newBuilder()
                     .setMessageIdentifier(UUID.randomUUID().toString())
                     .setAggregateIdentifier(UUID.randomUUID().toString())
                     .setPayload(serializedObject(null, null, "data2"))
                     .putMetaData("demo", metaDataValue("demoValue")).build(),
                                                        PLUGIN_KEY));


        Event orgEvent = event("aggregate1", 0, true);

        Event intercepted = testSubject.appendSnapshot(orgEvent, new TestExecutionContext("default"));
        assertEquals("sampleData", intercepted.getPayload().getData().toStringUtf8());
        assertFalse(intercepted.containsMetaData("demo"));

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        intercepted = testSubject.appendSnapshot(orgEvent, new TestExecutionContext("default"));

        assertEquals(orgEvent.getAggregateIdentifier(), intercepted.getAggregateIdentifier());
        assertEquals(orgEvent.getMessageIdentifier(), intercepted.getMessageIdentifier());
        assertEquals("data2", intercepted.getPayload().getData().toStringUtf8());
        assertTrue(intercepted.containsMetaData("demo"));
        assertEquals("demoValue",
                     intercepted.getMetaDataOrDefault("demo", MetaDataValue.getDefaultInstance()).getTextValue());
    }

    @Test
    public void noReadInterceptors() {
        pluginServiceProvider.add(new ServiceWithInfo<>((ReadEventInterceptor) (event, context) -> event,
                                                        PLUGIN_KEY));
        assertTrue(testSubject.noReadInterceptors("default"));
        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        assertFalse(testSubject.noReadInterceptors("default"));
    }

    @Test
    public void noReadInterceptorsWithSnapshotRead() {
        pluginServiceProvider.add(new ServiceWithInfo<>((ReadSnapshotInterceptor) (event, context) -> event,
                                                        PLUGIN_KEY));
        assertTrue(testSubject.noReadInterceptors("default"));
        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        assertFalse(testSubject.noReadInterceptors("default"));
    }

    @Test
    public void readSnapshot() {
        pluginServiceProvider.add(new ServiceWithInfo<>((ReadSnapshotInterceptor) (event, context) ->
                Event.newBuilder(event)
                     .putMetaData("intercepted", metaDataValue("yes"))
                     .setAggregateIdentifier(UUID.randomUUID().toString())
                     .build(), PLUGIN_KEY));

        Event event = event("sample", 0, true);
        ExecutionContext executionContext = new TestExecutionContext("default");
        Event result = testSubject.readSnapshot(event, executionContext);
        assertFalse(result.containsMetaData("intercepted"));

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        result = testSubject.readSnapshot(event, executionContext);
        assertEquals("yes", result.getMetaDataOrDefault("intercepted", metaDataValue("no")).getTextValue());
        assertEquals(event.getAggregateIdentifier(), result.getAggregateIdentifier());
        assertTrue(event.getSnapshot());
    }

    @Test
    public void readEvent() {
        pluginServiceProvider.add(new ServiceWithInfo<>((ReadEventInterceptor) (event, context) ->
                Event.newBuilder(event)
                     .putMetaData("intercepted", metaDataValue("yes"))
                     .setAggregateIdentifier(UUID.randomUUID().toString())
                     .build(), PLUGIN_KEY));

        Event event = event("sample", 0);
        ExecutionContext executionContext = new TestExecutionContext("default");
        Event result = testSubject.readEvent(event, executionContext);
        assertFalse(result.containsMetaData("intercepted"));

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        result = testSubject.readEvent(event, executionContext);
        assertEquals("yes", result.getMetaDataOrDefault("intercepted", metaDataValue("no")).getTextValue());
        assertEquals(event.getAggregateIdentifier(), result.getAggregateIdentifier());
    }

    @Test
    public void checkOrdering() {
        List<Integer> calledInOrder = new LinkedList<>();
        pluginServiceProvider.add(new ServiceWithInfo<>(new ReadEventInterceptor() {
            private static final int ORDER = 100;

            @Override
            public Event readEvent(Event event, ExecutionContext executionContext) {
                calledInOrder.add(ORDER);
                return event;
            }

            @Override
            public int order() {
                return ORDER;
            }
        }, PLUGIN_KEY));
        pluginServiceProvider.add(new ServiceWithInfo<>(new ReadEventInterceptor() {
            private static final int ORDER = 5;

            @Override
            public Event readEvent(Event event, ExecutionContext executionContext) {
                calledInOrder.add(ORDER);
                return event;
            }

            @Override
            public int order() {
                return ORDER;
            }
        }, PLUGIN_KEY));

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));

        ExecutionContext executionContext = new TestExecutionContext("default");
        testSubject.readEvent(event("a", 1), executionContext);
        assertEquals(2, calledInOrder.size());
        assertEquals(5, (int) calledInOrder.get(0));
        assertEquals(100, (int) calledInOrder.get(1));
    }

    @Test
    public void noEventReadInterceptors() {
        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        pluginServiceProvider.add(new ServiceWithInfo<>((ReadSnapshotInterceptor) (event, context) -> event,
                                                        PLUGIN_KEY));
        assertTrue(testSubject.noEventReadInterceptors("default"));
        pluginServiceProvider.add(new ServiceWithInfo<>((ReadEventInterceptor) (event, context) -> event,
                                                        PLUGIN_KEY));
        assertFalse(testSubject.noEventReadInterceptors("default"));
    }

    @Nonnull
    private MetaDataValue metaDataValue(String value) {
        return MetaDataValue.newBuilder().setTextValue(value).build();
    }

    @Nonnull
    private SerializedObject serializedObject(String type, String revision, String data) {
        return SerializedObject.newBuilder()
                               .setRevision(getOrDefault(revision, ""))
                               .setType(getOrDefault(type, ""))
                               .setData(ByteString.copyFromUtf8(data))
                               .build();
    }

    private Event event(String aggregateId, int aggregateSeqnr) {
        return event(aggregateId, aggregateSeqnr, false);
    }

    private Event event(String aggregateId, int aggregateSeqnr, boolean snapshot) {
        return Event.newBuilder()
                    .setMessageIdentifier(UUID.randomUUID().toString())
                    .setAggregateIdentifier(aggregateId)
                    .setAggregateSequenceNumber(aggregateSeqnr)
                    .setAggregateType("type1")
                    .setTimestamp(1000)
                    .setPayload(serializedObject("type2", "1.0", "sampleData"))
                    .setSnapshot(snapshot)
                    .build();
    }
}