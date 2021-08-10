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
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

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
    Mono<Void> appendEvents(String context, Flux<SerializedEvent> events, Authentication authentication);

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
     * Gets the token of the first event in event store.
     *
     * @param context the context in which the token will be searched for
     * @return a mono of the token
     */
    Mono<Long> firstEventToken(String context);

    /**
     * Gets the token of the last event in event store.
     *
     * @param context the context in which the token will be searched for
     * @return a mono of the token
     */
    Mono<Long> lastEventToken(String context);

    /**
     * Gets the token of the event at specific position in time.
     *
     * @param context   the context in which the token will be searched for
     * @param timestamp the timestamp to search the tracking token
     * @return a mono of the token
     */
    Mono<Long> eventTokenAt(String context, Instant timestamp);

    /**
     * Gets the highest sequence number of an aggregate for a given {@code aggregateId}.
     *
     * @param context     the context in which to search for the aggregate
     * @param aggregateId aggregate identifier
     * @return the highest sequence number of an aggregate or -1 if aggregate does not exist in given context
     */
    Mono<Long> highestSequenceNumber(String context, String aggregateId);

    /**
     * Queries this event store based on the provided {@code query}.
     *
     * @param context        the context in which to query
     * @param query          the flux of queries
     * @param authentication the authentication
     * @return a flux of results of the query
     */
    Flux<QueryEventsResponse> queryEvents(String context, Flux<QueryEventsRequest> query,
                                          Authentication authentication);

    /**
     * Deletes all event data in a given context (Only intended for development environments).
     *
     * @param context the context to be deleted
     * @return a Mono indicating when deletion is done
     */
    Mono<Void> deleteAllEventData(String context);
}
