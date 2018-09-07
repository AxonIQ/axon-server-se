package io.axoniq.axonserver.message.event;

import io.axoniq.axondb.Event;
import io.axoniq.axondb.grpc.Confirmation;
import io.axoniq.axondb.grpc.GetAggregateEventsRequest;
import io.axoniq.axondb.grpc.GetEventsRequest;
import io.axoniq.axondb.grpc.GetFirstTokenRequest;
import io.axoniq.axondb.grpc.GetLastTokenRequest;
import io.axoniq.axondb.grpc.GetTokenAtRequest;
import io.axoniq.axondb.grpc.QueryEventsRequest;
import io.axoniq.axondb.grpc.QueryEventsResponse;
import io.axoniq.axondb.grpc.ReadHighestSequenceNrRequest;
import io.axoniq.axondb.grpc.ReadHighestSequenceNrResponse;
import io.axoniq.axondb.grpc.TrackingToken;
import io.axoniq.axonserver.KeepNames;
import io.grpc.stub.StreamObserver;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

/**
 * Author: marc
 */
@KeepNames
public interface EventStore {
    CompletableFuture<Confirmation> appendSnapshot(String context, Event eventMessage);

    StreamObserver<Event> createAppendEventConnection(String context,
                                                      StreamObserver<Confirmation> responseObserver);

    void listAggregateEvents(String context, GetAggregateEventsRequest request,
                             StreamObserver<InputStream> responseStreamObserver);

    StreamObserver<GetEventsRequest> listEvents(String context, StreamObserver<InputStream> responseStreamObserver);

    void getFirstToken(String context, GetFirstTokenRequest request, StreamObserver<TrackingToken> responseObserver);

    void getLastToken(String context, GetLastTokenRequest request, StreamObserver<TrackingToken> responseObserver);

    void getTokenAt(String context, GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver);

    void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
                               StreamObserver<ReadHighestSequenceNrResponse> responseObserver);

    StreamObserver<QueryEventsRequest> queryEvents(String context, StreamObserver<QueryEventsResponse> responseObserver);
}
