/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;


/*import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.connector.event.EventTransformation;
import io.axoniq.axonserver.connector.event.EventTransformationChannel;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventTransformationServiceGrpc;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.*;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;*/

public class EventStoreTransformationGrpcControllerITCase {


    /*private static final AxonServerConnectionFactory connectionManager =
            AxonServerConnectionFactory.forClient("compoonent", "client").build();

    private static AxonServerConnection connection;


    private final static ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 8124)
                                                                     .usePlaintext()
                                                                     .build();

    private final static EventTransformationServiceGrpc.EventTransformationServiceStub transformation =
            EventTransformationServiceGrpc.newStub(channel);

    private final static UUID key = UUID.randomUUID();
    private static long firstToken;

    @BeforeClass
    public static void createEvents() throws ExecutionException, InterruptedException, TimeoutException {
        connection = connectionManager.connect("default");
        EventChannel eventChannel = connection.eventChannel();
        firstToken = eventChannel.getLastToken().get();


        Event[] events = IntStream.range(0, 100)
                                  .mapToObj(EventStoreTransformationGrpcControllerITCase::event)
                                  .toArray(Event[]::new);
        eventChannel.appendEvents(events).get(5, TimeUnit.SECONDS);
    }

    @AfterClass
    public static void stop() {
        connection.disconnect();
    }

    private static Event event(int i) {
        return Event.newBuilder()
                    .setAggregateIdentifier("Aggregate-" + key + "-" + i)
                    .setAggregateType("DemoAggregate")
                    .setMessageIdentifier(UUID.randomUUID().toString())
                    .setTimestamp(System.currentTimeMillis())
                    .setPayload(SerializedObject.newBuilder()
                                                .setData(ByteString.copyFromUtf8("This is the payload"))
                                                .setType("String")
                                                .build())
                    .build();
    }


    @Test
    public void usingWhen() {
        Flux<String> input = Flux.just("one", "two", "three", "one");
        Disposable countsPerInput = Flux.usingWhen(Mono.empty(),
                                                   v -> input.groupBy(eventWithToken -> {
                                                                 System.out.printf("getSegmentFor(%s)%n",
                                                                                   eventWithToken);
                                                                 return eventWithToken;
                                                             })
                                                             .flatMap(Flux::count),
                                                   v -> {
                                                       return Mono.fromRunnable(() -> System.out.println(v));
                                                   }).subscribe(e -> {
            System.out.println("Received: " + e);
        }, Throwable::printStackTrace);
    }

    @Test
    public void replaceEvents() throws ExecutionException, InterruptedException {
        EventChannel eventChannel = connection.eventChannel();
        EventTransformationChannel transformationChannel = connection.eventTransformationChannel();
        cancelActiveTransformation(transformationChannel);
        EventTransformation transformation = transformationChannel.newTransformation("Sample Transformation " + key)
                                                                  .get();

        AtomicInteger count = new AtomicInteger();
        try (EventStream stream = eventChannel.openStream(firstToken + 1, 100)) {
            EventWithToken next = stream.nextIfAvailable(1, TimeUnit.SECONDS);
            while (next != null && count.getAndIncrement() < 100) {
                if (next.getToken() % 3 == 0) {
                    System.out.println("Update " + next.getToken());
                    Event updated = Event.newBuilder(next.getEvent()).putMetaData("Update",
                                                                                  MetaDataValue.newBuilder()
                                                                                               .setTextValue("Updated")
                                                                                               .build()).build();
                    transformation.replaceEvent(next.getToken(), updated).get();
                }
                next = stream.nextIfAvailable(1, TimeUnit.SECONDS);
            }

            transformation.apply().get();
        }

        sleep(TimeUnit.SECONDS.toMillis(30));
//        assertWithin(1, TimeUnit.MINUTES, () -> isApplied(transformationChannel, transformation.id()));

        count.set(0);
        try (EventStream stream = eventChannel.openStream(firstToken + 1, 100)) {
            EventWithToken next = stream.nextIfAvailable(1, TimeUnit.SECONDS);
            while (next != null) {
                if (next.getToken() % 3 == 0) {
                    System.out.println(next.getToken());
                    assertTrue(next.getEvent().containsMetaData("Update"));
                } else {
                    assertFalse(next.getEvent().containsMetaData("Update"));
                }
                next = stream.nextIfAvailable(1, TimeUnit.SECONDS);
            }
        }
    }

    private void isApplied(EventTransformationChannel transformationChannel, EventTransformation.TransformationId id) {
        try {
            List<EventTransformation> activeTransformations = transformationChannel.transformations().get();
            assertTrue(activeTransformations.stream()
                                            .anyMatch(e -> e.id().id().equals(id.id()) &&
                                                    e.state().equals(EventTransformation.TransformationState.APPLIED)));
        } catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void cancelActiveTransformation(EventTransformationChannel transformationChannel)
            throws InterruptedException, ExecutionException {
        List<EventTransformation> activeTransformations = transformationChannel.transformations().get();
        activeTransformations.forEach(t -> {
            switch (t.state()) {
                case ACTIVE:
                    try {
                        t.cancel().get();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e.getCause());
                    }
                    break;
                case CANCELLING:
                case CANCELLED:
                case APPLYING:
                case APPLIED:
                case FATAL:
                case UNKNOWN:
                    break;
            }
        });
    }*/
}