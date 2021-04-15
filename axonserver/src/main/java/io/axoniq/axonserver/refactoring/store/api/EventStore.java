/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.store.api;

import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.GetFirstTokenRequest;
import io.axoniq.axonserver.grpc.event.GetLastTokenRequest;
import io.axoniq.axonserver.grpc.event.GetTokenAtRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrRequest;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrResponse;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.refactoring.store.SerializedEvent;
import io.grpc.stub.StreamObserver;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Provides a facade to the event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public interface EventStore {

    /**
     * Stores a snapshot in the event store.
     *
     * @param context  the context where the snapshot are stored
     * @param snapshot the snapshot
     * @return completable future that completes when snapshot is stored
     */
    @Deprecated
    CompletableFuture<Confirmation> appendSnapshot(String context, Authentication authentication, Event snapshot);

    /**
     * Stores a snapshot in the event store.
     *
     * @param context  the context where the snapshot are stored
     * @param snapshot the snapshot
     * @return completable future that completes when snapshot is stored
     */
    default Mono<Confirmation> appendSnapshot(String context,
                                              Authentication authentication,
                                              Snapshot snapshot) {
        return Mono.empty();
    }

    /**
     * Creates a connection that receives events to be stored in a single transaction.
     *
     * @param context          the context where the events are stored
     * @param responseObserver response stream where the event store can confirm completion of the transaction
     * @return stream to send events to
     */
    @Deprecated
    StreamObserver<InputStream> createAppendEventConnection(String context,
                                                            Authentication authentication,
                                                            StreamObserver<Confirmation> responseObserver);

    /**
     * Creates a connection that receives events to be stored in a single transaction.
     *
     * @param context the context where the events are stored
     * @param events  stream of events to be appended
     * @return stream to send events to
     */
    default Mono<Confirmation> appendEvents(String context,
                                            Authentication authentication,
                                            Flux<io.axoniq.axonserver.refactoring.store.api.Event> events) {
        return Mono.empty();
    }

    /**
     * Returns a {@link Flux} of all {@link SerializedEvent}s for an aggregate according whit the specified request.
     * The events could start with a snapshot event, if the request allows the usage of the snapshots. All the events
     * should have a sequential sequence number.
     *
     * @param context        the context containing the aggregate
     * @param authentication the authentication
     * @param request        the request containing the aggregate identifier and read options
     * @return a {@link Flux} of all {@link SerializedEvent}s for an aggregate according whit the specified request.
     */
    @Deprecated
    Flux<SerializedEvent> aggregateEvents(String context,
                                          Authentication authentication,
                                          GetAggregateEventsRequest request);


    /**
     * Returns a {@link Flux} of all {@link SerializedEvent}s for an aggregate according whit the specified request.
     * The events could start with a snapshot event, if the request allows the usage of the snapshots. All the events
     * should have a sequential sequence number.
     *
     * @param authentication the authentication
     * @param request        the request containing the aggregate identifier and read options
     * @return a {@link Flux} of all {@link SerializedEvent}s for an aggregate according whit the specified request.
     */
    default Flux<io.axoniq.axonserver.refactoring.store.api.Event> aggregateEvents(Authentication authentication,
                                                                                   AggregateEventsQuery request) {
        return Flux.empty();
    }

    /**
     * Returns a {@link Flux} of all snapshots events for an aggregate according whit the specified request. The
     * snapshots are sorted from the latest one. The snapshot sequence numbers are not sequential.
     *
     * @param context        the context containing the aggregate
     * @param authentication the authentication
     * @param request        the request containing the aggregate identifier and read options
     * @return a {@link Flux} of all {@link SerializedEvent}s for an aggregate according whit the specified request.
     */
    default Flux<SerializedEvent> aggregateSnapshots(String context,
                                                     Authentication authentication,
                                                     GetAggregateSnapshotsRequest request) {
        return Flux.create(
                sink -> listAggregateSnapshots(context, authentication, request, new StreamObserver<SerializedEvent>() {
                    @Override
                    public void onNext(SerializedEvent serializedEvent) {
                        sink.next(serializedEvent);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        sink.error(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        sink.complete();
                    }
                }));
    }

    /**
     * Returns a {@link Flux} of all snapshots events for an aggregate according whit the specified request. The
     * snapshots are sorted from the latest one. The snapshot sequence numbers are not sequential.
     *
     * @param authentication the authentication
     * @param request        the request containing the aggregate identifier and read options
     * @return a {@link Flux} of all {@link SerializedEvent}s for an aggregate according whit the specified request.
     */
    default Flux<Snapshot> aggregateSnapshots(Authentication authentication,
                                              AggregateSnapshotsQuery request) {
        return Flux.empty();
    }

    /**
     * Retrieves the Events from a given tracking token. Results are streamed rather than returned at once. Caller gets
     * a stream where it first should send the base request to (including the first token and a number of permits) and
     * subsequently send additional permits or blacklist messages to.
     *
     * @param context                the context to read from
     * @param responseStreamObserver {@link StreamObserver} where the events will be published
     * @return stream to send initial request and additional control messages to
     */
    StreamObserver<GetEventsRequest> listEvents(String context, Authentication authentication,
                                                StreamObserver<InputStream> responseStreamObserver);

    /**
     * Retrieves the Events from a given tracking token. Results are streamed rather than returned at once. Caller gets
     * a stream where it first should send the base request to (including the first token and a number of permits) and
     * subsequently send additional permits or blacklist messages to.
     *
     * @param request
     * @return stream of events with token
     */
    default Flux<EventWithToken> listEvents(Authentication authentication,
                                            EventsQuery request,
                                            Flux<PayloadType> blackListUpdates) {
        return Flux.empty();
    }

    @Deprecated
    void getFirstToken(String context, GetFirstTokenRequest request, StreamObserver<TrackingToken> responseObserver);

    default Mono<Long> getFirstEventToken(String context) {
        return Mono.empty();
    }

    @Deprecated
    void getLastToken(String context, GetLastTokenRequest request, StreamObserver<TrackingToken> responseObserver);

    default Mono<Long> getLastEventToken(String context) {
        return Mono.empty();
    }

    @Deprecated
    void getTokenAt(String context, GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver);

    default Mono<Long> getTokenAt(String context, Instant timestamp) {
        return Mono.empty();
    }

    @Deprecated
    void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
                               StreamObserver<ReadHighestSequenceNrResponse> responseObserver);

    default Mono<Long> readHighestSequenceNumber(String context, String aggregateId) {
        return Mono.empty();
    }

    @Deprecated
    StreamObserver<QueryEventsRequest> queryEvents(String context, Authentication authentication,
                                                   StreamObserver<QueryEventsResponse> responseObserver);

    default Flux<EventQueryResponse> queryEvents(Authentication authentication, AdHocEventsQuery query) {
        return Flux.empty();
    }


    default Flux<EventQueryResponse> querySnapshots(Authentication authentication, AdHocSnapshotsQuery query) {
        return Flux.empty();
    }

    @Deprecated
    void listAggregateSnapshots(String context, Authentication authentication, GetAggregateSnapshotsRequest request,
                                StreamObserver<SerializedEvent> responseObserver);

    /**
     * Deletes all event data in a given context (Only intended for development environments).
     *
     * @param context the context to be deleted
     */
    @Deprecated
    void deleteAllEventData(String context);

    /**
     * Deletes all event data in a given context (Only intended for development environments).
     *
     * @param context the context to be deleted
     */
    default Mono<Void> deleteAll(String context) {
        return Mono.empty();
    }
}
