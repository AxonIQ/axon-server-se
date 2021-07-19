/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

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
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.grpc.stub.StreamObserver;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
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
     * @param context        the context where the snapshot are stored
     * @param snapshot       the snapshot
     * @param authentication the authentication
     * @return completable future that completes when snapshot is stored
     */
    Mono<Void> appendSnapshot(String context, Event snapshot, Authentication authentication);

    /**
     * Creates a connection that receives events to be stored in a single transaction.
     *
     * @param context          the context where the events are stored
     * @param responseObserver response stream where the event store can confirm completion of the transaction
     * @return stream to send events to
     */
    StreamObserver<InputStream> createAppendEventConnection(String context,
                                                            Authentication authentication,
                                                            StreamObserver<Confirmation> responseObserver);

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

    void getFirstToken(String context, GetFirstTokenRequest request, StreamObserver<TrackingToken> responseObserver);

    void getLastToken(String context, GetLastTokenRequest request, StreamObserver<TrackingToken> responseObserver);

    void getTokenAt(String context, GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver);

    void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
                               StreamObserver<ReadHighestSequenceNrResponse> responseObserver);

    StreamObserver<QueryEventsRequest> queryEvents(String context, Authentication authentication,
                                                   StreamObserver<QueryEventsResponse> responseObserver);

    void listAggregateSnapshots(String context, Authentication authentication, GetAggregateSnapshotsRequest request,
                                StreamObserver<SerializedEvent> responseObserver);

    /**
     * Deletes all event data in a given context (Only intended for development environments).
     *
     * @param context the context to be deleted
     */
    void deleteAllEventData(String context);
}
