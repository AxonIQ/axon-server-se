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
import io.axoniq.axonserver.extensions.ExtensionKey;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.RequestRejectedException;
import io.axoniq.axonserver.extensions.ServiceWithInfo;
import io.axoniq.axonserver.extensions.hook.PostCommitEventsHook;
import io.axoniq.axonserver.extensions.hook.PostCommitSnapshotHook;
import io.axoniq.axonserver.extensions.hook.PreCommitEventsHook;
import io.axoniq.axonserver.extensions.interceptor.AppendEventInterceptor;
import io.axoniq.axonserver.extensions.interceptor.AppendSnapshotInterceptor;
import io.axoniq.axonserver.extensions.interceptor.ReadEventInterceptor;
import io.axoniq.axonserver.extensions.interceptor.ReadSnapshotInterceptor;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import org.junit.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

import static io.axoniq.axonserver.util.StringUtils.getOrDefault;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class DefaultEventInterceptorsTest {

    public static final ExtensionKey EXTENSION_KEY = new ExtensionKey("sample", "1.0");
    private final TestExtensionServiceProvider osgiController = new TestExtensionServiceProvider();
    private final ExtensionContextFilter extensionContextFilter = new ExtensionContextFilter(osgiController);
    private final DefaultEventInterceptors testSubject = new DefaultEventInterceptors(extensionContextFilter);


    @Test
    public void appendEvent() {
        osgiController.add(new ServiceWithInfo<>((AppendEventInterceptor) (event, extensionContext) ->
                Event.newBuilder()
                     .setMessageIdentifier(UUID.randomUUID().toString())
                     .setAggregateIdentifier(UUID.randomUUID().toString())
                     .setPayload(serializedObject(null, null, "data2"))
                     .putMetaData("demo", metaDataValue("demoValue")).build(),
                                                 EXTENSION_KEY));


        Event orgEvent = event("aggregate1", 0);

        Event intercepted = testSubject.appendEvent(orgEvent, new TestExtensionUnitOfWork("default"));
        assertEquals("sampleData", intercepted.getPayload().getData().toStringUtf8());
        assertFalse(intercepted.containsMetaData("demo"));

        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        intercepted = testSubject.appendEvent(orgEvent, new TestExtensionUnitOfWork("default"));

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
        osgiController.add(new ServiceWithInfo<>((PreCommitEventsHook) (events, context) -> {
            hookCalled.incrementAndGet();
        }, EXTENSION_KEY));

        TestExtensionUnitOfWork testExtensionUnitOfWork = new TestExtensionUnitOfWork("default");
        testSubject.eventsPreCommit(Arrays.asList(event("aggrId1", 0), event("aggrId1", 1)), testExtensionUnitOfWork);
        assertEquals(0, hookCalled.get());

        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        testSubject.eventsPreCommit(Arrays.asList(event("aggrId1", 0), event("aggrId1", 1)), testExtensionUnitOfWork);
        assertEquals(1, hookCalled.get());
    }


    @Test
    public void eventsPostCommit() {
        AtomicInteger hookCalled = new AtomicInteger();
        osgiController.add(new ServiceWithInfo<>((PostCommitEventsHook) (events, context) -> {
            hookCalled.incrementAndGet();
        }, EXTENSION_KEY));

        TestExtensionUnitOfWork testExtensionUnitOfWork = new TestExtensionUnitOfWork("default");
        testSubject.eventsPostCommit(Arrays.asList(event("aggrId1", 0), event("aggrId1", 1)), testExtensionUnitOfWork);
        assertEquals(0, hookCalled.get());

        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        testSubject.eventsPostCommit(Arrays.asList(event("aggrId1", 0), event("aggrId1", 1)), testExtensionUnitOfWork);
        assertEquals(1, hookCalled.get());
    }

    @Test
    public void eventsPostCommitWithException() {
        osgiController.add(new ServiceWithInfo<>((PostCommitEventsHook) (events, context) -> {
            throw new RuntimeException("Error in post commit hook");
        }, EXTENSION_KEY));

        TestExtensionUnitOfWork testExtensionUnitOfWork = new TestExtensionUnitOfWork("default");
        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        testSubject.eventsPostCommit(Arrays.asList(event("aggrId1", 0), event("aggrId1", 1)), testExtensionUnitOfWork);
    }

    @Test
    public void snapshotPostCommitWithException() {
        osgiController.add(new ServiceWithInfo<>((PostCommitSnapshotHook) (events, context) -> {
            throw new RuntimeException("Error in post commit hook");
        }, EXTENSION_KEY));

        TestExtensionUnitOfWork testExtensionUnitOfWork = new TestExtensionUnitOfWork("default");
        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        testSubject.snapshotPostCommit(event("aggrId1", 0), testExtensionUnitOfWork);
    }

    @Test
    public void snapshotPostCommit() {
        AtomicInteger hookCalled = new AtomicInteger();
        osgiController.add(new ServiceWithInfo<>((PostCommitSnapshotHook) (events, context) -> {
            hookCalled.incrementAndGet();
        }, EXTENSION_KEY));

        TestExtensionUnitOfWork testExtensionUnitOfWork = new TestExtensionUnitOfWork("default");
        testSubject.snapshotPostCommit(event("aggrId1", 0), testExtensionUnitOfWork);
        assertEquals(0, hookCalled.get());

        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        testSubject.snapshotPostCommit(event("aggrId1", 0), testExtensionUnitOfWork);
        assertEquals(1, hookCalled.get());
    }

    @Test
    public void appendSnapshot() {
        osgiController.add(new ServiceWithInfo<>((AppendSnapshotInterceptor) (event, extensionContext) ->
                Event.newBuilder()
                     .setMessageIdentifier(UUID.randomUUID().toString())
                     .setAggregateIdentifier(UUID.randomUUID().toString())
                     .setPayload(serializedObject(null, null, "data2"))
                     .putMetaData("demo", metaDataValue("demoValue")).build(),
                                                 EXTENSION_KEY));


        Event orgEvent = event("aggregate1", 0, true);

        Event intercepted = testSubject.appendSnapshot(orgEvent, new TestExtensionUnitOfWork("default"));
        assertEquals("sampleData", intercepted.getPayload().getData().toStringUtf8());
        assertFalse(intercepted.containsMetaData("demo"));

        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        intercepted = testSubject.appendSnapshot(orgEvent, new TestExtensionUnitOfWork("default"));

        assertEquals(orgEvent.getAggregateIdentifier(), intercepted.getAggregateIdentifier());
        assertEquals(orgEvent.getMessageIdentifier(), intercepted.getMessageIdentifier());
        assertEquals("data2", intercepted.getPayload().getData().toStringUtf8());
        assertTrue(intercepted.containsMetaData("demo"));
        assertEquals("demoValue",
                     intercepted.getMetaDataOrDefault("demo", MetaDataValue.getDefaultInstance()).getTextValue());
    }

    @Test
    public void noReadInterceptors() {
        osgiController.add(new ServiceWithInfo<>((ReadEventInterceptor) (event, context) -> event, EXTENSION_KEY));
        assertTrue(testSubject.noReadInterceptors("default"));
        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        assertFalse(testSubject.noReadInterceptors("default"));
    }

    @Test
    public void noReadInterceptorsWithSnapshotRead() {
        osgiController.add(new ServiceWithInfo<>((ReadSnapshotInterceptor) (event, context) -> event, EXTENSION_KEY));
        assertTrue(testSubject.noReadInterceptors("default"));
        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        assertFalse(testSubject.noReadInterceptors("default"));
    }

    @Test
    public void readSnapshot() {
        osgiController.add(new ServiceWithInfo<>((ReadSnapshotInterceptor) (event, context) -> {
            return Event.newBuilder(event).putMetaData("intercepted", metaDataValue("yes"))
                        .setAggregateIdentifier(UUID.randomUUID().toString()).build();
        }, EXTENSION_KEY));

        Event event = event("sample", 0, true);
        ExtensionUnitOfWork unitOfWork = new TestExtensionUnitOfWork("default");
        Event result = testSubject.readSnapshot(event, unitOfWork);
        assertFalse(result.containsMetaData("intercepted"));

        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        result = testSubject.readSnapshot(event, unitOfWork);
        assertEquals("yes", result.getMetaDataOrDefault("intercepted", metaDataValue("no")).getTextValue());
        assertEquals(event.getAggregateIdentifier(), result.getAggregateIdentifier());
        assertTrue(event.getSnapshot());
    }

    @Test
    public void readEvent() {
        osgiController.add(new ServiceWithInfo<>((ReadEventInterceptor) (event, context) -> {
            return Event.newBuilder(event).putMetaData("intercepted", metaDataValue("yes"))
                        .setAggregateIdentifier(UUID.randomUUID().toString()).build();
        }, EXTENSION_KEY));

        Event event = event("sample", 0);
        ExtensionUnitOfWork unitOfWork = new TestExtensionUnitOfWork("default");
        Event result = testSubject.readEvent(event, unitOfWork);
        assertFalse(result.containsMetaData("intercepted"));

        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        result = testSubject.readEvent(event, unitOfWork);
        assertEquals("yes", result.getMetaDataOrDefault("intercepted", metaDataValue("no")).getTextValue());
        assertEquals(event.getAggregateIdentifier(), result.getAggregateIdentifier());
    }

    @Test
    public void checkOrdering() {
        List<Integer> calledInOrder = new LinkedList<>();
        osgiController.add(new ServiceWithInfo<>(new ReadEventInterceptor() {
            private static final int ORDER = 100;

            @Override
            public Event readEvent(Event event, ExtensionUnitOfWork extensionContext) {
                calledInOrder.add(ORDER);
                return event;
            }

            @Override
            public int order() {
                return ORDER;
            }
        }, EXTENSION_KEY));
        osgiController.add(new ServiceWithInfo<>(new ReadEventInterceptor() {
            private static final int ORDER = 5;

            @Override
            public Event readEvent(Event event, ExtensionUnitOfWork extensionContext) {
                calledInOrder.add(ORDER);
                return event;
            }

            @Override
            public int order() {
                return ORDER;
            }
        }, EXTENSION_KEY));

        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));

        ExtensionUnitOfWork unitOfWork = new TestExtensionUnitOfWork("default");
        testSubject.readEvent(event("a", 1), unitOfWork);
        assertEquals(2, calledInOrder.size());
        assertEquals(5, (int) calledInOrder.get(0));
        assertEquals(100, (int) calledInOrder.get(1));
    }

    @Test
    public void noEventReadInterceptors() {
        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        osgiController.add(new ServiceWithInfo<>((ReadSnapshotInterceptor) (event, context) -> event, EXTENSION_KEY));
        assertTrue(testSubject.noEventReadInterceptors("default"));
        osgiController.add(new ServiceWithInfo<>((ReadEventInterceptor) (event, context) -> event, EXTENSION_KEY));
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