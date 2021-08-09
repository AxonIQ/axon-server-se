/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.axoniq.axonserver.message.event.EventStoreTransformationService;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class EventTransformationServiceTest {

    private static final String CONTEXT = "CONTEXT";
    private static final Authentication AUTHENTICATION = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
    private EventTransformationService testSubject;

    @Before
    public void setUp() throws Exception {
        testSubject = new EventTransformationService(() -> CONTEXT,
                                                     () -> AUTHENTICATION,
                                                     eventStoreTransformationService());
    }

    private EventStoreTransformationService eventStoreTransformationService() {
        return new EventStoreTransformationService() {
            private final Map<String, String> activeTransformations = new HashMap<>();

            @Override
            public Mono<String> startTransformation(String context) {
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
            public Mono<Void> transformEvents(String context, Flux<TransformEventsRequest> flux) {
                return Mono.create(sink ->
                                           flux.subscribe(request -> {
                                               if (request.hasTransformationId() && !request.getTransformationId()
                                                                                            .getId().equals(
                                                               activeTransformations.get(context))) {
                                                   sink.error(new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                                                             "Transformation not found"));
                                               }
                                           }, error -> {
                                                sink.error(error);
                                           }, () -> {
                                                sink.success(null);
                                           }));
            }

            @Override
            public Mono<Void> cancelTransformation(String context, String id) {
                return Mono.create(sink -> {
                    if (id.equals(activeTransformations.get(context))) {
                        sink.success();
                        return;
                    }
                    sink.error(new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND, "Transformation not found"));
                });
            }

            @Override
            public Mono<Void> applyTransformation(String context, String id) {
                return Mono.create(sink -> {
                    if (id.equals(activeTransformations.get(context))) {
                        sink.success();
                        return;
                    }
                    sink.error(new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND, "Transformation not found"));
                });
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
        StreamObserver<TransformEventsRequest> requestStream = testSubject.transformEvents(new CompletableFutureStreamObserver<>(
                futureConfirmation));
        requestStream.onNext(TransformEventsRequest.newBuilder()
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
        testSubject.startTransformation(Empty.getDefaultInstance(),
                                        new CompletableFutureStreamObserver<>(futureTransformationId));

        return futureTransformationId.get();
    }

    @Test
    public void transformEventsError() throws InterruptedException {
        TransformationId transformationId = TransformationId.newBuilder().setId("unknown").build();
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        StreamObserver<TransformEventsRequest> requestStream = testSubject.transformEvents(new CompletableFutureStreamObserver<>(
                futureConfirmation));
        requestStream.onNext(TransformEventsRequest.newBuilder()
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
        testSubject.applyTransformation(transformationId, new CompletableFutureStreamObserver<>(futureConfirmation));
        assertTrue(futureConfirmation.get().getSuccess());
    }

    @Test
    public void applyTransformationError() throws InterruptedException {
        TransformationId transformationId = TransformationId.newBuilder().setId("unknown").build();
        CompletableFuture<Confirmation> futureConfirmation = new CompletableFuture<>();
        testSubject.applyTransformation(transformationId, new CompletableFutureStreamObserver<>(futureConfirmation));
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
}
