/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

/**
 * Component that executes scheduled events. Stores the events in the event store when the scheduled time has arrived.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class ScheduledEventExecutor implements ScheduledTask {

    private final LocalEventStore localEventStore;

    /**
     * Constructs the component.
     * @param localEventStore the event store facade
     */
    public ScheduledEventExecutor(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    /**
     * Executes the scheduled task to store an event. The payload contains the protobuf serialized event message.
     * @param context the context in which to store the event
     * @param payload the payload for the task
     * @return completable future that completes when event is successfully stored
     */
    @Override
    public CompletableFuture<Void> executeAsync(String context, Object payload) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            ScheduledEventWrapper scheduledEventWrapper = (ScheduledEventWrapper) payload;
            Event event = Event.newBuilder(Event.parseFrom(scheduledEventWrapper.getBytes()))
                               .setTimestamp(System.currentTimeMillis()).build();
            StreamObserver<InputStream> inputStream = localEventStore.createAppendEventConnection(scheduledEventWrapper
                                                                                                          .getContext(),
                                                                                                  null,
                                                                                                  new StreamObserver<Confirmation>() {
                                                                                                      @Override
                                                                                                      public void onNext(
                                                                                                              Confirmation confirmation) {
                                                                                                          result.complete(
                                                                                                                  null);
                                                                                                      }

                                                                                                      @Override
                                                                                                      public void onError(
                                                                                                              Throwable throwable) {
                                                                                                          result.completeExceptionally(
                                                                                                                  throwable);
                                                                                                      }

                                                                                                      @Override
                                                                                                      public void onCompleted() {

                                                                                                      }
                                                                                                  });

            inputStream.onNext(new SerializedEvent(event).asInputStream());
            inputStream.onCompleted();
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
        return result;
    }

}
