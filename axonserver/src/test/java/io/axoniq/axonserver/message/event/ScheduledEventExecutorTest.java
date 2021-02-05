/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.mockito.stubbing.*;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class ScheduledEventExecutorTest {

    private ScheduledEventExecutor testSubject;
    private LocalEventStore localEventStore = mock(LocalEventStore.class);

    @Before
    public void setUp() {
        when(localEventStore.createAppendEventConnection(anyString(), any(), any()))
                .then((Answer<StreamObserver<InputStream>>) invocation -> {
                    StreamObserver<Confirmation> responseStream = invocation.getArgument(2);
                    return new StreamObserver<InputStream>() {
                        @Override
                        public void onNext(InputStream inputStream) {
                            responseStream.onNext(Confirmation.newBuilder().setSuccess(true).build());
                            responseStream.onCompleted();
                        }

                        @Override
                        public void onError(Throwable throwable) {

                        }

                        @Override
                        public void onCompleted() {

                        }
                    };
                });
        testSubject = new ScheduledEventExecutor(localEventStore);
    }

    @Test
    public void executeAsync() {
        ScheduledEventWrapper payload = new ScheduledEventWrapper(Topology.DEFAULT_CONTEXT,
                                                                  Event.newBuilder().build().toByteArray());
        CompletableFuture<Void> result = testSubject.executeAsync("context", payload);
        try {
            result.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted");
        } catch (ExecutionException e) {
            e.printStackTrace();
            fail(e.getCause().getMessage());
        } catch (TimeoutException e) {
            fail("Timeout waiting for result");
        }
    }

    @Test
    public void executeAsyncInvalidPayload() {
        SerializedObject payload = SerializedObject.newBuilder()
                                                   .setData(ByteString.copyFromUtf8("Some random string"))
                                                   .setType(Command.class.getName())
                                                   .build();
        CompletableFuture<Void> result = testSubject.executeAsync("context", payload);
        try {
            result.get(1, TimeUnit.SECONDS);
            fail("Should not succeed on non-event payload");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted");
        } catch (ExecutionException e) {

        } catch (TimeoutException e) {
            fail("Timeout waiting for result");
        }
    }
}