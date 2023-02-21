/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.CompactionRequest;
import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.StartTransformationRequest;
import io.axoniq.axonserver.grpc.event.TransformRequest;
import io.axoniq.axonserver.grpc.event.TransformRequestAck;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.axoniq.axonserver.transport.grpc.EventStoreTransformationGrpcController;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import org.mockito.*;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class EventStoreTransformationGrpcControllerTest {

    private static final String CONTEXT = "CONTEXT";
    private static final Authentication AUTHENTICATION = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
    private final EventStoreTransformationService mockedService = mock(EventStoreTransformationService.class);
    private final EventStoreTransformationGrpcController testSubject = new EventStoreTransformationGrpcController(() -> CONTEXT,
                                                                                                                  () -> AUTHENTICATION,
                                                                                                                  mockedService);


    @Test
    void testStartTransformation() throws ExecutionException, InterruptedException {
        CompletableFuture<TransformationId> transformationIdCompletableFuture = new CompletableFuture<>();
        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        when(mockedService.start(idCaptor.capture(),
                                 eq(CONTEXT),
                                 eq("My Description"),
                                 any())).thenReturn(Mono.empty());
        testSubject.startTransformation(StartTransformationRequest.newBuilder()
                                                                  .setDescription("My Description")
                                                                  .build(),
                                        new CompletableFutureStreamObserver<>(transformationIdCompletableFuture));
        Assertions.assertEquals(transformationIdCompletableFuture.get().getId(), idCaptor.getValue());
    }

    @Test
    void testDeleteEvent() {
        when(mockedService.deleteEvent(eq(CONTEXT), eq("transformation-ID"), eq(100L), eq(5L), any()))
                .thenReturn(Mono.empty());
        CompletableFuture<TransformRequestAck> future = new CompletableFuture<>();
        CompletableFutureStreamObserver<TransformRequestAck> response = new CompletableFutureStreamObserver<>(future);
        StreamObserver<TransformRequest> streamObserver = testSubject.transformEvents(response);
        streamObserver.onNext(TransformRequest.newBuilder()
                                              .setTransformationId(TransformationId.newBuilder()
                                                                                   .setId("transformation-ID"))
                                              .setDeleteEvent(DeletedEvent.newBuilder()
                                                                          .setToken(100L))
                                              .setSequence(5L)
                                              .build());
        streamObserver.onCompleted();
        //TODO test backpressure
    }

    @Test
    void testReplaceEvent() {
        when(mockedService.replaceEvent(eq(CONTEXT), eq("transformation-ID"),
                                        eq(100L), eq(Event.getDefaultInstance()),
                                        eq(5L), any()))
                .thenReturn(Mono.empty());
        CompletableFuture<TransformRequestAck> future = new CompletableFuture<>();
        CompletableFutureStreamObserver<TransformRequestAck> response = new CompletableFutureStreamObserver<>(future);
        StreamObserver<TransformRequest> streamObserver = testSubject.transformEvents(response);
        streamObserver.onNext(TransformRequest.newBuilder()
                                              .setTransformationId(TransformationId.newBuilder()
                                                                                   .setId("transformation-ID"))
                                              .setReplaceEvent(TransformedEvent.newBuilder()
                                                                               .setEvent(Event.getDefaultInstance())
                                                                               .setToken(100L))
                                              .setSequence(5L)
                                              .build());
        streamObserver.onCompleted();
    }

    @Test
    void testCancelTransformation() {
        when(mockedService.cancel(eq(CONTEXT), eq("My-ID"), any())).thenReturn(Mono.empty());
        CompletableFuture<Empty> future = new CompletableFuture<>();
        testSubject.cancelTransformation(TransformationId.newBuilder().setId("My-ID").build(),
                                         new CompletableFutureStreamObserver<>(future));
    }

    @Test
    void testApplyTransformation() {
        when(mockedService.startApplying(eq(CONTEXT), eq("My-ID"), eq(8L), any())).thenReturn(Mono.empty());
        CompletableFuture<Empty> future = new CompletableFuture<>();
        testSubject.applyTransformation(ApplyTransformationRequest.newBuilder().setTransformationId(
                                                                          TransformationId.newBuilder().setId("My-ID"))
                                                                  .setLastSequence(8L)
                                                                  .build(),
                                        new CompletableFutureStreamObserver<>(future));
    }

    @Test
    void testCompact() {
        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        when(mockedService.startCompacting(idCaptor.capture(), eq(CONTEXT), any()))
                .thenReturn(Mono.empty());
        CompletableFuture<Empty> future = new CompletableFuture<>();
        testSubject.compact(CompactionRequest.getDefaultInstance(),
                            new CompletableFutureStreamObserver<>(future));
    }

    public static class CompletableFutureStreamObserver<T> extends CallStreamObserver<T> {

        private final CompletableFuture<T> future;

        public CompletableFutureStreamObserver(CompletableFuture<T> future) {
            this.future = future;
        }

        @Override
        public void onNext(T next) {
            this.future.complete(next);
        }

        @Override
        public void onError(Throwable throwable) {
            future.completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {
            future.complete(null);
        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setOnReadyHandler(Runnable onReadyHandler) {

        }

        @Override
        public void disableAutoInboundFlowControl() {

        }

        @Override
        public void request(int count) {

        }

        @Override
        public void setMessageCompression(boolean enable) {

        }
    }
}
