package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.grpc.AxonServerInternalService;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventStoreGrpc;
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
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.message.event.ForwardingStreamObserver;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.axonserver.message.event.EventDispatcher.*;
import static io.grpc.stub.ServerCalls.*;

/**
 * @author Marc Gathier
 */
@Controller
public class InternalEventStoreService implements AxonServerInternalService {
    private final LocalEventStore localEventStore;
    private final ContextProvider contextProvider;

    private static Logger logger = LoggerFactory.getLogger(InternalEventStoreService.class);

    public InternalEventStoreService(LocalEventStore localEventStore,
                                     ContextProvider contextProvider) {
        this.localEventStore = localEventStore;
        this.contextProvider = contextProvider;
    }

    @Override
    public boolean requiresContextInterceptor() {
        return true;
    }

    @Override
    public final ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(EventStoreGrpc.SERVICE_NAME)
                                              .addMethod(
                                                      METHOD_APPEND_EVENT,
                                                      asyncClientStreamingCall( this::appendEvent))
                                              .addMethod(
                                                      EventStoreGrpc.getAppendSnapshotMethod(),
                                                      asyncUnaryCall(
                                                              this::appendSnapshot))
                                              .addMethod(
                                                      METHOD_LIST_AGGREGATE_EVENTS,
                                                      asyncServerStreamingCall(this::listAggregateEvents))
                                              .addMethod(
                                                      METHOD_LIST_EVENTS,
                                                      asyncBidiStreamingCall(this::listEvents))
                                              .addMethod(
                                                      METHOD_LIST_AGGREGATE_SNAPSHOTS,
                                                      asyncServerStreamingCall(this::listAggregateSnapshots))
                                              .addMethod(
                                                      EventStoreGrpc.getReadHighestSequenceNrMethod(),
                                                      asyncUnaryCall(this::readHighestSequenceNr))
                                              .addMethod(
                                                      EventStoreGrpc.getGetFirstTokenMethod(),
                                                      asyncUnaryCall(this::getFirstToken))
                                              .addMethod(
                                                      EventStoreGrpc.getGetLastTokenMethod(),
                                                      asyncUnaryCall(this::getLastToken))
                                              .addMethod(
                                                      EventStoreGrpc.getGetTokenAtMethod(),
                                                      asyncUnaryCall(this::getTokenAt))
                                              .addMethod(
                                                      EventStoreGrpc.getQueryEventsMethod(),
                                                      asyncBidiStreamingCall(this::queryEvents))
                                              .build();
    }


    private StreamObserver<InputStream> appendEvent(StreamObserver<Confirmation> responseObserver) {
        return localEventStore.createAppendEventConnection(
                contextProvider.getContext(),
                new ForwardingStreamObserver<>(logger, "appendEvent", responseObserver));
    }

    private void appendSnapshot(Event request, StreamObserver<Confirmation> responseObserver) {
        CompletableFuture<Confirmation> response = localEventStore.appendSnapshot(contextProvider
                                                                                          .getContext(),
                                                                                  request);
        response.whenComplete((confirmation, throwable) -> {
            if( throwable == null) {
                responseObserver.onNext(confirmation);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(GrpcExceptionBuilder.build(throwable));
            }
        });
    }

    private void listAggregateEvents(GetAggregateEventsRequest request, StreamObserver<InputStream> responseObserver) {
        localEventStore.listAggregateEvents(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, "listAggregateEvents", responseObserver));
    }

    private void listAggregateSnapshots(GetAggregateSnapshotsRequest request, StreamObserver<InputStream> responseObserver) {
        localEventStore.listAggregateSnapshots(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, "listAggregateSnapshots", responseObserver));
    }

    private StreamObserver<GetEventsRequest> listEvents(StreamObserver<InputStream> responseObserver) {
        return localEventStore.listEvents(contextProvider.getContext(), new ForwardingStreamObserver<>(logger, "listEvents", responseObserver));
    }

    private void readHighestSequenceNr(ReadHighestSequenceNrRequest request,
                                      StreamObserver<ReadHighestSequenceNrResponse> responseObserver) {
        localEventStore.readHighestSequenceNr(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, "readHighestSequenceNr", responseObserver));
    }

    private StreamObserver<QueryEventsRequest> queryEvents(StreamObserver<QueryEventsResponse> responseObserver) {
        return localEventStore.queryEvents(contextProvider.getContext(), new ForwardingStreamObserver<>(logger, "queryEvents", responseObserver));
    }

    private void getFirstToken(GetFirstTokenRequest request, StreamObserver<TrackingToken> responseObserver) {
        localEventStore.getFirstToken(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, "getFirstToken", responseObserver));
    }

    private void getLastToken(GetLastTokenRequest request, StreamObserver<TrackingToken> responseObserver) {
        localEventStore.getLastToken(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, "getLastToken", responseObserver));
    }

    private void getTokenAt(GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver) {
        localEventStore.getTokenAt(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, "getTokenAt", responseObserver));
    }
}
