/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.StartTransformationRequest;
import io.axoniq.axonserver.grpc.event.TransformEventRequest;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.axoniq.axonserver.transport.grpc.EventStoreTransformationGrpcController;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class EventStoreTransformationGrpcControllerTest {

    private static final String CONTEXT = "CONTEXT";
    private static final Authentication AUTHENTICATION = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
    private EventStoreTransformationGrpcController testSubject;

    @Before
    public void setUp() throws Exception {
        testSubject = new EventStoreTransformationGrpcController(() -> CONTEXT,
                                                                 () -> AUTHENTICATION,
                                                                 eventStoreTransformationService());
    }

    private EventStoreTransformationService eventStoreTransformationService() {
        return new EventStoreTransformationService() {
            private final Map<String, String> activeTransformations = new HashMap<>();

            @Override
            public Mono<String> startTransformation(String context, String description,
                                                    @Nonnull io.axoniq.axonserver.api.Authentication authentication) {
                return Mono.create(sink -> {
                    if (activeTransformations.containsKey(context)) {
                        sink.error(new MessagingPlatformException(
                                ErrorCode.CONTEXT_EXISTS,
                                "Transformation for context in progress"));
                        return;
                    }
                    String id = activeTransformations.compute(context, (c, old) -> UUID.randomUUID().toString());
                    sink.success(id);
                });
            }

            @Override
            public Mono<Void> deleteEvent(String context, String transformationId, long token, long previousToken,
                                          @Nonnull io.axoniq.axonserver.api.Authentication authentication) {
                if (transformationId != null && !transformationId.equals(
                        activeTransformations.get(context))) {
                    return Mono.error(new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                                     "Transformation not found"));
                }
                return Mono.empty();
            }

            @Override
            public Mono<Void> replaceEvent(String context, String transformationId, long token, Event event,
                                           long previousToken,
                                           @Nonnull io.axoniq.axonserver.api.Authentication authentication) {
                if (transformationId != null && !transformationId.equals(
                        activeTransformations.get(context))) {
                    return Mono.error(new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                                     "Transformation not found"));
                }
                return Mono.empty();
            }

            @Override
            public Mono<Void> cancelTransformation(String context, String id,
                                                   @Nonnull io.axoniq.axonserver.api.Authentication authentication) {
                return Mono.create(sink -> {
                    if (id.equals(activeTransformations.get(context))) {
                        sink.success();
                        return;
                    }
                    sink.error(new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND, "Transformation not found"));
                });
            }

            @Override
            public Mono<Void> applyTransformation(String context, String id, long lastEventToken,
                                                  boolean keepOldVersions,
                                                  @Nonnull io.axoniq.axonserver.api.Authentication authentication) {
                return Mono.create(sink -> {
                    if (id.equals(activeTransformations.get(context))) {
                        sink.success();
                        return;
                    }
                    sink.error(new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND, "Transformation not found"));
                });
            }

            @Override
            public Mono<Void> rollbackTransformation(String context, String id,
                                                     @Nonnull io.axoniq.axonserver.api.Authentication authentication) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> deleteOldVersions(String context, String id,
                                                @Nonnull io.axoniq.axonserver.api.Authentication authentication) {
                return Mono.empty();
            }
        };
    }

    @Test
    public void startTransformation() throws ExecutionException, InterruptedException {
        TransformationId id = doStartTransformation();
        assertNotNull(id.getId());
    }

    @Test
    public void startTransformationFails() throws ExecutionException, InterruptedException {
        doStartTransformation();
        try {
            doStartTransformation();
            fail("Start transaction should fail");
        } catch (ExecutionException executionException) {
            assertTrue(executionException.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    public void transformEvents() throws ExecutionException, InterruptedException {
        TransformationId transformationId = doStartTransformation();
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        StreamObserver<TransformEventRequest> requestStream = testSubject.transformEvents(new FlowControlledCompletableFutureStreamObserver<>(
                futureConfirmation));
        requestStream.onNext(TransformEventRequest.newBuilder()
                                                   .setTransformationId(transformationId)
                                                   .setEvent(TransformedEvent.newBuilder()
                                                                             .setToken(1)
                                                                             .setEvent(Event.getDefaultInstance())
                                                                             .build())
                                                   .build());
        requestStream.onCompleted();
        Confirmation confirmation = futureConfirmation.get();
        assertTrue(confirmation.getSuccess());
    }

    private TransformationId doStartTransformation() throws InterruptedException, ExecutionException {
        CompletableFuture<TransformationId> futureTransformationId = new CompletableFuture<>();
        testSubject.startTransformation(StartTransformationRequest.getDefaultInstance(),
                                        new CompletableFutureStreamObserver<>(futureTransformationId));

        return futureTransformationId.get();
    }

    @Test
    public void transformEventsError() throws InterruptedException {
        TransformationId transformationId = TransformationId.newBuilder().setId("unknown").build();
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        StreamObserver<TransformEventRequest> requestStream = testSubject.transformEvents(new FlowControlledCompletableFutureStreamObserver<>(
                futureConfirmation));
        requestStream.onNext(TransformEventRequest.newBuilder()
                                                   .setTransformationId(transformationId)
                                                   .setEvent(TransformedEvent.newBuilder()
                                                                             .setToken(1)
                                                                             .setEvent(Event.getDefaultInstance())
                                                                             .build())
                                                   .build());
        requestStream.onCompleted();
        try {
            futureConfirmation.get();
            fail("Should fail");
        } catch (ExecutionException executionException) {
            assertTrue(executionException.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    public void cancelTransformation() throws ExecutionException, InterruptedException {
        TransformationId transformationId = doStartTransformation();
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        testSubject.cancelTransformation(transformationId, new CompletableFutureStreamObserver<>(futureConfirmation));
        assertTrue(futureConfirmation.get().getSuccess());
    }
    @Test
    public void cancelTransformationError() throws InterruptedException {
        TransformationId transformationId = TransformationId.newBuilder().setId("unknown").build();
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        testSubject.cancelTransformation(transformationId, new CompletableFutureStreamObserver<>(futureConfirmation));
        try {
            futureConfirmation.get();
            fail("Expected exception");
        } catch (ExecutionException executionException) {
            assertTrue(executionException.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    public void applyTransformation() throws ExecutionException, InterruptedException {
        TransformationId transformationId = doStartTransformation();
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        testSubject.applyTransformation(ApplyTransformationRequest.newBuilder().setTransformationId(transformationId).build(), new CompletableFutureStreamObserver<>(futureConfirmation));
        assertTrue(futureConfirmation.get().getSuccess());
    }

    @Test
    public void applyTransformationError() throws InterruptedException {
        TransformationId transformationId = TransformationId.newBuilder().setId("unknown").build();
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        testSubject.applyTransformation(ApplyTransformationRequest.newBuilder().setTransformationId(transformationId).build(), new CompletableFutureStreamObserver<>(futureConfirmation));
        try {
            futureConfirmation.get();
            fail("Expected exception");
        } catch (ExecutionException executionException) {
            assertTrue(executionException.getCause() instanceof StatusRuntimeException);
        }
    }

    private static class CompletableFutureStreamObserver<T> implements StreamObserver<T> {

        private final CompletableFuture<T> futureTransactionId;
        volatile T transformationId;

        public CompletableFutureStreamObserver(
                CompletableFuture<T> futureTransactionId) {
            this.futureTransactionId = futureTransactionId;
        }

        @Override
        public void onNext(T transformationId) {
            this.transformationId = transformationId;
        }

        @Override
        public void onError(Throwable throwable) {
            futureTransactionId.completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {
            futureTransactionId.complete(transformationId);
        }
    }

    private static class FlowControlledCompletableFutureStreamObserver<T> extends CallStreamObserver<T> {

        private final CompletableFuture<T> futureTransactionId;
        volatile T transformationId;

        public FlowControlledCompletableFutureStreamObserver(
                CompletableFuture<T> futureTransactionId) {
            super();
            this.futureTransactionId = futureTransactionId;
        }

        @Override
        public void onNext(T transformationId) {
            this.transformationId = transformationId;
        }

        @Override
        public void onError(Throwable throwable) {
            futureTransactionId.completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {
            futureTransactionId.complete(transformationId);
        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setOnReadyHandler(Runnable runnable) {
        }

        @Override
        public void disableAutoInboundFlowControl() {
        }

        @Override
        public void request(int i) {
        }

        @Override
        public void setMessageCompression(boolean b) {
        }
    }
}
