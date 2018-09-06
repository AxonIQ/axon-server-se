package io.axoniq.axonhub.grpc.internal;

import io.axoniq.axondb.Event;
import io.axoniq.axondb.grpc.Confirmation;
import io.axoniq.axondb.grpc.EventStoreGrpc;
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
import io.axoniq.axonhub.grpc.ContextProvider;
import io.axoniq.axonhub.localstorage.LocalEventStore;
import io.axoniq.axonhub.message.event.InputStreamMarshaller;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Controller;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import static io.grpc.stub.ServerCalls.*;

/**
 * Author: marc
 */
@Controller
public class InternalEventStoreService implements BindableService {
    private final LocalEventStore localEventStore;
    private final ContextProvider contextProvider;

    public static final MethodDescriptor<GetEventsRequest, InputStream> METHOD_LIST_EVENTS =
            EventStoreGrpc.METHOD_LIST_EVENTS.toBuilder(
                    ProtoUtils.marshaller(GetEventsRequest.getDefaultInstance()),
                    InputStreamMarshaller.inputStreamMarshaller())
                                             .build();
    public static final MethodDescriptor<GetAggregateEventsRequest, InputStream> METHOD_LIST_AGGREGATE_EVENTS =
            EventStoreGrpc.METHOD_LIST_AGGREGATE_EVENTS.toBuilder(
                    ProtoUtils.marshaller(GetAggregateEventsRequest.getDefaultInstance()),
                    InputStreamMarshaller.inputStreamMarshaller())
                                                       .build();

    public InternalEventStoreService(LocalEventStore localEventStore,
                                     ContextProvider contextProvider) {
        this.localEventStore = localEventStore;
        this.contextProvider = contextProvider;
    }

    @Override
    public final ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(EventStoreGrpc.SERVICE_NAME)
                                              .addMethod(
                                                      EventStoreGrpc.METHOD_APPEND_EVENT,
                                                      asyncClientStreamingCall( this::appendEvent))
                                              .addMethod(
                                                      EventStoreGrpc.METHOD_APPEND_SNAPSHOT,
                                                      asyncUnaryCall(
                                                              this::appendSnapshot))
                                              .addMethod(
                                                      METHOD_LIST_AGGREGATE_EVENTS,
                                                      asyncServerStreamingCall(this::listAggregateEvents))
                                              .addMethod(
                                                      METHOD_LIST_EVENTS,
                                                      asyncBidiStreamingCall(this::listEvents))
                                              .addMethod(
                                                      EventStoreGrpc.METHOD_READ_HIGHEST_SEQUENCE_NR,
                                                      asyncUnaryCall(this::readHighestSequenceNr))
                                              .addMethod(
                                                      EventStoreGrpc.METHOD_GET_FIRST_TOKEN,
                                                      asyncUnaryCall(this::getFirstToken))
                                              .addMethod(
                                                      EventStoreGrpc.METHOD_GET_LAST_TOKEN,
                                                      asyncUnaryCall(this::getLastToken))
                                              .addMethod(
                                                      EventStoreGrpc.METHOD_GET_TOKEN_AT,
                                                      asyncUnaryCall(this::getTokenAt))
                                              .addMethod(
                                                      EventStoreGrpc.METHOD_QUERY_EVENTS,
                                                      asyncBidiStreamingCall(this::queryEvents))
                                              .build();
    }


    public StreamObserver<Event> appendEvent(StreamObserver<Confirmation> responseObserver) {
        return localEventStore.createAppendEventConnection(
                contextProvider.getContext(),
                responseObserver);
    }

    public void appendSnapshot(Event request, StreamObserver<Confirmation> responseObserver) {
        CompletableFuture<Confirmation> response = localEventStore.appendSnapshot(contextProvider
                                                                                          .getContext(),
                                                                                  request);
        response.whenComplete((confirmation, throwable) -> {
            if( throwable == null) {
                responseObserver.onNext(confirmation);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(throwable);
            }
        });
    }

    public void listAggregateEvents(GetAggregateEventsRequest request, StreamObserver<InputStream> responseObserver) {
        localEventStore.listAggregateEvents(contextProvider.getContext(), request, responseObserver);
    }

    public StreamObserver<GetEventsRequest> listEvents(StreamObserver<InputStream> responseObserver) {
        return localEventStore.listEvents(contextProvider.getContext(), responseObserver);
    }

    public void readHighestSequenceNr(ReadHighestSequenceNrRequest request,
                                      StreamObserver<ReadHighestSequenceNrResponse> responseObserver) {
        localEventStore.readHighestSequenceNr(contextProvider.getContext(), request, responseObserver);
    }

    public StreamObserver<QueryEventsRequest> queryEvents(StreamObserver<QueryEventsResponse> responseObserver) {
        return localEventStore.queryEvents(contextProvider.getContext(), responseObserver);
    }

    public void getFirstToken(GetFirstTokenRequest request, StreamObserver<TrackingToken> responseObserver) {
        localEventStore.getFirstToken(contextProvider.getContext(), request, responseObserver);
    }

    public void getLastToken(GetLastTokenRequest request, StreamObserver<TrackingToken> responseObserver) {
        localEventStore.getLastToken(contextProvider.getContext(), request, responseObserver);
    }

    public void getTokenAt(GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver) {
        localEventStore.getTokenAt(contextProvider.getContext(), request, responseObserver);
    }
}
