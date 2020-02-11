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
import io.grpc.stub.StreamObserver;

import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * @author Marc Gathier
 */
public interface EventStore {
    CompletableFuture<Confirmation> appendSnapshot(String context, Event eventMessage);

    StreamObserver<InputStream> createAppendEventConnection(String context,
                                                            StreamObserver<Confirmation> responseObserver);

    void listAggregateEvents(String context, GetAggregateEventsRequest request,
                             StreamObserver<InputStream> responseStreamObserver);

    StreamObserver<GetEventsRequest> listEvents(String context, StreamObserver<InputStream> responseStreamObserver);

    void getFirstToken(String context, GetFirstTokenRequest request, StreamObserver<TrackingToken> responseObserver);

    void getLastToken(String context, GetLastTokenRequest request, StreamObserver<TrackingToken> responseObserver);

    void getTokenAt(String context, GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver);

    void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
                               StreamObserver<ReadHighestSequenceNrResponse> responseObserver);

    StreamObserver<QueryEventsRequest> queryEvents(String context,
                                                   StreamObserver<QueryEventsResponse> responseObserver);

    void listAggregateSnapshots(String context, GetAggregateSnapshotsRequest request,
                                StreamObserver<InputStream> responseObserver);

    /**
     * Deletes all event data in a given context (Only intended for development environments).
     *
     * @param context the context to be deleted
     */
    void deleteAllEventData(String context);

    Optional<Long> getLastSequenceNumber(String context, String aggregateIdentifier, int maxNrOfSegments);
}
