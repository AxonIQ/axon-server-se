/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.axoniq.axonserver.grpc.EventStoreTransformationGrpcControllerTest.CompletableFutureStreamObserver;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.CompactionRequest;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventStoreGrpc;
import io.axoniq.axonserver.grpc.event.EventTransformationServiceGrpc;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.StartTransformationRequest;
import io.axoniq.axonserver.grpc.event.TransformRequest;
import io.axoniq.axonserver.grpc.event.TransformRequestAck;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

@Ignore("Skip as it depends on running Axon Server")
public class TransformationIntegrationTest {

    @Test
    public void createEventStore() throws ExecutionException, InterruptedException {
        Channel channel = ManagedChannelBuilder.forAddress("localhost", 8124).usePlaintext().build();
        EventStoreGrpc.EventStoreStub eventStoreStub = EventStoreGrpc.newStub(channel);
        for (int batch = 0; batch < 5000; batch++) {
            CompletableFuture<Confirmation> eventsAppended = new CompletableFuture<>();

            StreamObserver<Event> eventStream = eventStoreStub.appendEvent(new StreamObserver<Confirmation>() {
                @Override
                public void onNext(Confirmation confirmation) {
                    eventsAppended.complete(confirmation);
                }

                @Override
                public void onError(Throwable throwable) {
                    eventsAppended.completeExceptionally(throwable);
                }

                @Override
                public void onCompleted() {

                }
            });
            byte[] data = new byte[3000];
            Arrays.fill(data, (byte) 'q');
            for (int event = 0; event < 200; event++) {
                eventStream.onNext(Event.newBuilder()
                                        .setMessageIdentifier(UUID.randomUUID().toString())
                                        .setTimestamp(System.currentTimeMillis())
                                        .setPayload(SerializedObject.newBuilder()
                                                                    .setData(ByteString.copyFrom(data))
                                                                    .setType("string")
                                                                    .build())
                                        .build());
            }
            eventStream.onCompleted();
            eventsAppended.get();
        }
    }

    @Test
    public void transformationTest2() throws ExecutionException, InterruptedException {
        Channel channel = ManagedChannelBuilder.forAddress("localhost", 8124).usePlaintext().build();
        EventTransformationServiceGrpc.EventTransformationServiceStub transformationServiceStub =
                EventTransformationServiceGrpc.newStub(channel);

        CompletableFuture<TransformationId> transformationStarted = new CompletableFuture<>();
        transformationServiceStub.startTransformation(StartTransformationRequest.newBuilder()
                                                                                .setDescription("Test transformation")
                                                                                .build(),
                                                      new StreamObserver<>() {
                                                          @Override
                                                          public void onNext(TransformationId transformationId) {
                                                              transformationStarted.complete(transformationId);
                                                          }

                                                          @Override
                                                          public void onError(Throwable throwable) {
                                                              transformationStarted.completeExceptionally(throwable);
                                                          }

                                                          @Override
                                                          public void onCompleted() {

                                                          }
                                                      });


        TransformationId transformationId = transformationStarted.get();
        System.out.println(transformationId.getId());
        CompletableFuture<Void> transformedEvent = new CompletableFuture<>();
        StreamObserver<TransformRequest> transformationStream = transformationServiceStub.transformEvents(
                new StreamObserver<>() {
                    @Override
                    public void onNext(TransformRequestAck transformRequestAck) {

                    }

                    @Override
                    public void onError(Throwable throwable) {
                        throwable.printStackTrace();
                        transformedEvent.completeExceptionally(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        transformedEvent.complete(null);
                    }
                });

        long[] tokens = {
                0, 1, 2, 3, 4, 87600, 87601, 87602, 87603, 87604, 87605, 175200, 175201, 175202, 175203, 175204
        };
        for (int i = 0; i < tokens.length; i++) {
            transformationStream.onNext(transformRequest(transformationId, i, tokens[i], event()));
        }
        transformationStream.onCompleted();
        transformedEvent.get();

        CompletableFuture<Empty> applied = new CompletableFuture<>();
        transformationServiceStub.applyTransformation(
                ApplyTransformationRequest.newBuilder()
                                          .setTransformationId(transformationId)
                                          .setLastSequence(tokens.length - 1)
                                          .build(), new CompletableFutureStreamObserver<>(applied));
        applied.get();
    }

    @Test
    public void deleteEvents() throws ExecutionException, InterruptedException {
        Channel channel = ManagedChannelBuilder.forAddress("localhost", 8124).usePlaintext().build();
        EventTransformationServiceGrpc.EventTransformationServiceStub transformationServiceStub =
                EventTransformationServiceGrpc.newStub(channel);

        CompletableFuture<TransformationId> transformationStarted = new CompletableFuture<>();
        transformationServiceStub.startTransformation(StartTransformationRequest.newBuilder()
                                                                                .setDescription("Test transformation")
                                                                                .build(),
                                                      new StreamObserver<>() {
                                                          @Override
                                                          public void onNext(TransformationId transformationId) {
                                                              transformationStarted.complete(transformationId);
                                                          }

                                                          @Override
                                                          public void onError(Throwable throwable) {
                                                              transformationStarted.completeExceptionally(throwable);
                                                          }

                                                          @Override
                                                          public void onCompleted() {

                                                          }
                                                      });


        TransformationId transformationId = transformationStarted.get();
        System.out.println(transformationId.getId());
        CompletableFuture<Void> transformedEvent = new CompletableFuture<>();
        StreamObserver<TransformRequest> transformationStream = transformationServiceStub.transformEvents(
                new StreamObserver<>() {
                    @Override
                    public void onNext(TransformRequestAck transformRequestAck) {

                    }

                    @Override
                    public void onError(Throwable throwable) {
                        throwable.printStackTrace();
                        transformedEvent.completeExceptionally(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        transformedEvent.complete(null);
                    }
                });

        long start = 1_100_000;
        int count = 5;
        for (int i = 0; i < count; i++) {
            transformationStream.onNext(deleteRequest(transformationId, i, start + i));
        }
        transformationStream.onCompleted();
        transformedEvent.get();

        CompletableFuture<Empty> applied = new CompletableFuture<>();
        transformationServiceStub.applyTransformation(
                ApplyTransformationRequest.newBuilder()
                                          .setTransformationId(transformationId)
                                          .setLastSequence(count - 1)
                                          .build(), new CompletableFutureStreamObserver<>(applied));
        applied.get();
    }

    @Test
    public void compact() throws ExecutionException, InterruptedException {
        Channel channel = ManagedChannelBuilder.forAddress("localhost", 8124).usePlaintext().build();
        EventTransformationServiceGrpc.EventTransformationServiceStub transformationServiceStub =
                EventTransformationServiceGrpc.newStub(channel);

        CompletableFuture<Empty> compacted = new CompletableFuture<>();

        transformationServiceStub.compact(CompactionRequest.getDefaultInstance(),
                                          new CompletableFutureStreamObserver<>(compacted));
        compacted.get();
    }

    private Event event() {
        byte[] data = new byte[3000];
        Arrays.fill(data, (byte) 'a');
        return Event.newBuilder()
                    .setMessageIdentifier(UUID.randomUUID().toString())
                    .setTimestamp(System.currentTimeMillis())
                    .setPayload(SerializedObject.newBuilder()
                                                .setData(ByteString.copyFrom(data))
                                                .setType("string")
                                                .setRevision("1.1")
                                                .build())
                    .build();
    }

    @NotNull
    private TransformRequest transformRequest(TransformationId transformationId,
                                              long sequence,
                                              long token,
                                              Event event) {
        return TransformRequest.newBuilder()
                               .setTransformationId(transformationId)
                               .setSequence(sequence)
                               .setReplaceEvent(
                                       TransformedEvent.newBuilder()
                                                       .setToken(token)
                                                       .setEvent(event)).build();
    }

    @NotNull
    private TransformRequest deleteRequest(TransformationId transformationId,
                                           long sequence,
                                           long token) {
        return TransformRequest.newBuilder()
                               .setTransformationId(transformationId)
                               .setSequence(sequence)
                               .setDeleteEvent(
                                       DeletedEvent.newBuilder()
                                                   .setToken(token)).build();
    }

    @Test
    public void transformationTest() throws ExecutionException, InterruptedException {
        Channel channel = ManagedChannelBuilder.forAddress("localhost", 8124).usePlaintext().build();
        EventStoreGrpc.EventStoreStub eventStoreStub = EventStoreGrpc.newStub(channel);
        EventTransformationServiceGrpc.EventTransformationServiceStub transformationServiceStub =
                EventTransformationServiceGrpc.newStub(channel);

        CompletableFuture<TransformationId> transformationStarted = new CompletableFuture<>();
        transformationServiceStub.startTransformation(StartTransformationRequest.newBuilder()
                                                                                .setDescription("Test transformation")
                                                                                .build(),
                                                      new CompletableFutureStreamObserver<>(transformationStarted));


        TransformationId transformationId = transformationStarted.get();
        System.out.println(transformationId.getId());
        CompletableFuture<Void> transformedEvent = new CompletableFuture<>();
        StreamObserver<TransformRequest> transformationStream = transformationServiceStub.transformEvents(
                new StreamObserver<>() {
                    @Override
                    public void onNext(TransformRequestAck transformRequestAck) {

                    }

                    @Override
                    public void onError(Throwable throwable) {
                        throwable.printStackTrace();
                        transformedEvent.completeExceptionally(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        transformedEvent.complete(null);
                    }
                });

        CompletableFuture<EventWithToken> eventsReceived = new CompletableFuture<>();
        AtomicLong lastSequence = new AtomicLong();
        long size = 100;
        long start = 1_000_000;
        StreamObserver<GetEventsRequest> requestStore = eventStoreStub.listEvents(new StreamObserver<>() {
            @Override
            public void onNext(EventWithToken event) {
                System.out.println(event.getToken());
                if (transformedEvent.isDone()) {
                    return;
                }

                if (event.getToken() < start + size) {
                    transformationStream.onNext(transformRequest(transformationId,
                                                                 lastSequence.getAndIncrement(),
                                                                 event.getToken(),
                                                                 updateEvent(event)));
                }
                if (event.getToken() == start + size) {
                    eventsReceived.complete(event);
                    transformationStream.onCompleted();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                throwable.printStackTrace();
                eventsReceived.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {

            }
        });

        requestStore.onNext(GetEventsRequest.newBuilder()
                                            .setNumberOfPermits(size + 1)
                                            .setTrackingToken(start)
                                            .build());

        try {
            eventsReceived.get();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        transformedEvent.get();

        CompletableFuture<Empty> applied = new CompletableFuture<>();
        transformationServiceStub.applyTransformation(
                ApplyTransformationRequest.newBuilder()
                                          .setTransformationId(transformationId)
                                          .setLastSequence(lastSequence.get() - 1)
                                          .build(),
                new CompletableFutureStreamObserver<>(applied)
        );
        applied.get();
    }

    @NotNull
    private Event updateEvent(EventWithToken event) {
        return event.getEvent()
                    .toBuilder()
                    .setPayload(
                            event.getEvent()
                                 .getPayload()
                                 .toBuilder()
                                 .setRevision(
                                         "1.0"))
                    .build();
    }
}
