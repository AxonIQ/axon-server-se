package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.*;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.message.event.ForwardingStreamObserver;
import io.axoniq.axonserver.message.event.InputStreamMarshaller;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import static io.grpc.stub.ServerCalls.*;

/**
 * @author Marc Gathier
 */
@Controller
public class InternalEventStoreService implements BindableService {
    private final LocalEventStore localEventStore;
    private final ContextProvider contextProvider;

    private static final MethodDescriptor<GetEventsRequest, InputStream> METHOD_LIST_EVENTS =
            EventStoreGrpc.getListEventsMethod().toBuilder(
                    ProtoUtils.marshaller(GetEventsRequest.getDefaultInstance()),
                    InputStreamMarshaller.inputStreamMarshaller())
                                             .build();
    private static final MethodDescriptor<GetAggregateSnapshotsRequest, InputStream> METHOD_LIST_AGGREGATE_SNAPSHOTS =
            EventStoreGrpc.getListAggregateSnapshotsMethod().toBuilder(
                    ProtoUtils.marshaller(GetAggregateSnapshotsRequest.getDefaultInstance()),
                    InputStreamMarshaller.inputStreamMarshaller())
                                             .build();
    private static final MethodDescriptor<GetAggregateEventsRequest, InputStream> METHOD_LIST_AGGREGATE_EVENTS =
            EventStoreGrpc.getListAggregateEventsMethod().toBuilder(
                    ProtoUtils.marshaller(GetAggregateEventsRequest.getDefaultInstance()),
                    InputStreamMarshaller.inputStreamMarshaller())
                                                       .build();
    private static Logger logger = LoggerFactory.getLogger(InternalEventStoreService.class);

    public InternalEventStoreService(LocalEventStore localEventStore,
                                     ContextProvider contextProvider) {
        this.localEventStore = localEventStore;
        this.contextProvider = contextProvider;
    }

    @Override
    public final ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(EventStoreGrpc.SERVICE_NAME)
                                              .addMethod(
                                                      EventStoreGrpc.getAppendEventMethod(),
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


    private StreamObserver<Event> appendEvent(StreamObserver<Confirmation> responseObserver) {
        return localEventStore.createAppendEventConnection(
                contextProvider.getContext(),
                new ForwardingStreamObserver<>(logger, responseObserver));
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
        localEventStore.listAggregateEvents(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, responseObserver));
    }

    private void listAggregateSnapshots(GetAggregateSnapshotsRequest request, StreamObserver<InputStream> responseObserver) {
        localEventStore.listAggregateSnapshots(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, responseObserver));
    }

    private StreamObserver<GetEventsRequest> listEvents(StreamObserver<InputStream> responseObserver) {
        return localEventStore.listEvents(contextProvider.getContext(), new ForwardingStreamObserver<>(logger, responseObserver));
    }

    private void readHighestSequenceNr(ReadHighestSequenceNrRequest request,
                                      StreamObserver<ReadHighestSequenceNrResponse> responseObserver) {
        localEventStore.readHighestSequenceNr(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, responseObserver));
    }

    private StreamObserver<QueryEventsRequest> queryEvents(StreamObserver<QueryEventsResponse> responseObserver) {
        return localEventStore.queryEvents(contextProvider.getContext(), new ForwardingStreamObserver<>(logger, responseObserver));
    }

    private void getFirstToken(GetFirstTokenRequest request, StreamObserver<TrackingToken> responseObserver) {
        localEventStore.getFirstToken(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, responseObserver));
    }

    private void getLastToken(GetLastTokenRequest request, StreamObserver<TrackingToken> responseObserver) {
        localEventStore.getLastToken(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, responseObserver));
    }

    private void getTokenAt(GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver) {
        localEventStore.getTokenAt(contextProvider.getContext(), request, new ForwardingStreamObserver<>(logger, responseObserver));
    }
}
