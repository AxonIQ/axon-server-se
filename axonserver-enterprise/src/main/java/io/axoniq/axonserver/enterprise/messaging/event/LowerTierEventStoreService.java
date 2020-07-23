package io.axoniq.axonserver.enterprise.messaging.event;

import io.axoniq.axonserver.grpc.AxonServerInternalService;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.grpc.internal.GetHighestSequenceNumberRequest;
import io.axoniq.axonserver.grpc.internal.GetHighestSequenceNumberResponse;
import io.axoniq.axonserver.grpc.internal.GetLastSnapshotTokenRequest;
import io.axoniq.axonserver.grpc.internal.GetTransactionsRequest;
import io.axoniq.axonserver.grpc.internal.LowerTierEventStoreServiceGrpc;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of the {@link LowerTierEventStoreServiceGrpc} service.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Service
public class LowerTierEventStoreService extends LowerTierEventStoreServiceGrpc.LowerTierEventStoreServiceImplBase
        implements AxonServerInternalService {

    private final Logger logger = LoggerFactory.getLogger(LowerTierEventStoreService.class);
    private final LocalEventStore localEventStore;

    public LowerTierEventStoreService(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    /**
     * Returns transactions within a specific token range to the caller. Flow control on the streams.
     *
     * @param responseObserver observer to publish transactions
     * @return observer to receive the initial request and permits requests from the caller
     */
    @Override
    public StreamObserver<GetTransactionsRequest> listEvents(StreamObserver<TransactionWithToken> responseObserver) {
        return createFlowControlledStreamObserver(responseObserver,
                                                  request -> localEventStore.eventTransactionsIterator(
                                                          request.getContext(),
                                                          request.getTrackingToken(),
                                                          request.getToTrackingToken()));
    }

    @NotNull
    private StreamObserver<GetTransactionsRequest> createFlowControlledStreamObserver(
            StreamObserver<TransactionWithToken> responseObserver,
            Function<GetTransactionsRequest, CloseableIterator<SerializedTransactionWithToken>> iteratorBuilderFunction) {
        return new StreamObserver<GetTransactionsRequest>() {
            private final AtomicBoolean started = new AtomicBoolean();
            private final AtomicLong permitsLeft = new AtomicLong();
            private CloseableIterator<SerializedTransactionWithToken> iterator;
            private String context;

            @Override
            public void onNext(GetTransactionsRequest request) {
                logger.debug("{}: received new {} permits", context, request.getNumberOfPermits());
                if (started.compareAndSet(false, true)) {
                    this.context = request.getContext();
                    iterator = iteratorBuilderFunction.apply(request);
                    permitsLeft.set(request.getNumberOfPermits());
                    sendEvents();
                } else {
                    long before = permitsLeft.getAndAdd(request.getNumberOfPermits());
                    if (before <= 0) {
                        logger.debug("{}: starting to send events again {} permits, old {}",
                                     context,
                                     permitsLeft,
                                     before);
                        permitsLeft.getAndAdd(Math.abs(before));
                        sendEvents();
                    }
                }
            }

            private void sendEvents() {
                long permits = permitsLeft.get();
                while (permits > 0 && iterator.hasNext()) {
                    SerializedTransactionWithToken transactionWithToken = iterator.next();
                    responseObserver.onNext(TransactionWithToken.newBuilder()
                                                                .setContext(context)
                                                                .setToken(transactionWithToken.getToken())
                                                                .setVersion(transactionWithToken.getVersion())
                                                                .addAllEvents(transactionWithToken.getEvents().stream()
                                                                                                  .map(
                                                                                                          SerializedEvent::asByteString)
                                                                                                  .collect(
                                                                                                          Collectors
                                                                                                                  .toList()))
                                                                .build());
                    permits = permitsLeft.decrementAndGet();
                }
                if (!iterator.hasNext()) {
                    responseObserver.onCompleted();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                iterator.close();
            }

            @Override
            public void onCompleted() {
                iterator.close();
            }
        };
    }

    /**
     * Returns snapshot transactions within a specific token range to the caller. Flow control on the streams.
     *
     * @param responseObserver observer to publish transactions
     * @return observer to receive the initial request and permits requests from the caller
     */
    @Override
    public StreamObserver<GetTransactionsRequest> listSnapshots(StreamObserver<TransactionWithToken> responseObserver) {
        return createFlowControlledStreamObserver(responseObserver,
                                                  request -> localEventStore.snapshotTransactionsIterator(
                                                          request.getContext(),
                                                          request.getTrackingToken(),
                                                          request.getToTrackingToken()));
    }

    /**
     * Returns the highest sequence number for an aggregate. Returns -1 when there is no event found for the aggregate.
     *
     * @param request          the request containing the aggregate identifier and search options
     * @param responseObserver response observer where the result will be published
     */
    @Override
    public void getHighestSequenceNumber(GetHighestSequenceNumberRequest request,
                                         StreamObserver<GetHighestSequenceNumberResponse> responseObserver) {
        localEventStore.getHighestSequenceNr(request.getContext(), request.getAggregateIdentifier(),
                                             request.getMaxSegmentsHint(), request.getMaxTokenHint())
                       .thenAccept(seqnr -> {
                           responseObserver.onNext(GetHighestSequenceNumberResponse
                                                           .newBuilder()
                                                           .setSequenceNumber(seqnr)
                                                           .build());
                           StreamObserverUtils.complete(responseObserver);
                       }).exceptionally(throwable -> {
            StreamObserverUtils.error(responseObserver, GrpcExceptionBuilder.build(throwable));
            return null;
        });
    }

    /**
     * Returns the highest token from the snapshot store for a context.
     *
     * @param request          the request containing the context name
     * @param responseObserver stream for the tracking token response
     */
    @Override
    public void getLastSnapshotToken(GetLastSnapshotTokenRequest request,
                                     StreamObserver<TrackingToken> responseObserver) {

        responseObserver.onNext(TrackingToken.newBuilder()
                                             .setToken(localEventStore.getLastSnapshot(request.getContext()))
                                             .build());
        responseObserver.onCompleted();
    }
}
