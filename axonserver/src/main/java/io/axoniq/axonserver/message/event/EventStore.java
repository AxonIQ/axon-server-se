/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.GetLastTokenRequest;
import io.axoniq.axonserver.grpc.event.GetTokenAtRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrRequest;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrResponse;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.grpc.stub.StreamObserver;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
     * @param context        the context where the snapshot are stored
     * @param snapshot       the snapshot
     * @param authentication the authentication
     * @return completable future that completes when snapshot is stored
     */
    Mono<Void> appendSnapshot(String context, Event snapshot, Authentication authentication);

    /**
     * Creates a connection that receives events to be stored in a single transaction.
     *
     * @param context        the context where the events are stored
     * @param events         stream of events to be appended
     * @param authentication the authentication
     * @return stream to send events to
     */
    Mono<Void> appendEvents(String context, Flux<Event> events, Authentication authentication);

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
    Flux<SerializedEvent> aggregateEvents(String context,
                                          Authentication authentication,
                                          GetAggregateEventsRequest request);

    /**
     * Returns a {@link Flux} of all snapshots events for an aggregate according whit the specified request. The
     * snapshots are sorted from the latest one. The snapshot sequence numbers are not sequential.
     *
     * @param context        the context containing the aggregate
     * @param authentication the authentication
     * @param request        the request containing the aggregate identifier and read options
     * @return a {@link Flux} of all {@link SerializedEvent}s for an aggregate according whit the specified request.
     */
    Flux<SerializedEvent> aggregateSnapshots(String context,
                                             Authentication authentication,
                                             GetAggregateSnapshotsRequest request);

    /**
     * Retrieves the Events from a given tracking token. Results are streamed rather than returned at once. Results are
     * streamed using {@link Flux}.
     *
     * @param context        the context to read from
     * @param authentication the authentication
     * @param requestFlux    an input flux of request - meaning that request may change during events streaming
     * @return serialized events with their corresponding tokens
     */
    Flux<SerializedEventWithToken> events(String context,
                                          Authentication authentication,
                                          Flux<GetEventsRequest> requestFlux);

    /**
     * Gets the token of first event in event store.
     *
     * @param context the context in which the token will be searched for
     * @return a mono of the token
     */
    Mono<Long> firstEventToken(String context);

    void getLastToken(String context, GetLastTokenRequest request, StreamObserver<TrackingToken> responseObserver);

    void getTokenAt(String context, GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver);

    void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
                               StreamObserver<ReadHighestSequenceNrResponse> responseObserver);

    StreamObserver<QueryEventsRequest> queryEvents(String context, Authentication authentication,
                                                   StreamObserver<QueryEventsResponse> responseObserver);

    /**
     * Deletes all event data in a given context (Only intended for development environments).
     *
     * @param context the context to be deleted
     */
    void deleteAllEventData(String context);
}
