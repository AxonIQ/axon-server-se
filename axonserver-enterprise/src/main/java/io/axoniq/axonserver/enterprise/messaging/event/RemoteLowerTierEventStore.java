package io.axoniq.axonserver.enterprise.messaging.event;

import io.axoniq.axonserver.enterprise.cluster.CompletableStreamObserver;
import io.axoniq.axonserver.enterprise.cluster.internal.InternalTokenAddingInterceptor;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ChannelProvider;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.GetLastTokenRequest;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.grpc.internal.GetHighestSequenceNumberRequest;
import io.axoniq.axonserver.grpc.internal.GetHighestSequenceNumberResponse;
import io.axoniq.axonserver.grpc.internal.GetLastSnapshotTokenRequest;
import io.axoniq.axonserver.grpc.internal.GetTransactionsRequest;
import io.axoniq.axonserver.grpc.internal.LowerTierEventStoreServiceGrpc;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

/**
 * Implementation of the {@link LowerTierEventStore}. Extends the normal {@link RemoteEventStore} to implement
 * the additional operations.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class RemoteLowerTierEventStore extends RemoteEventStore implements LowerTierEventStore {

    private static final Logger logger = LoggerFactory.getLogger(RemoteLowerTierEventStore.class);
    /**
     * Initial number of permits granted in reading transactions from a lower tier node.
     */
    private static final long INITIAL_PERMITS = 10000;
    /**
     * Threshold at which the node will send another grant of newPermits to the connected platform node.
     */
    private static final long THRESHOLD = 5000;
    /**
     * Additional number of permits granted in communication between axonserver platform nodes.
     */
    private static final long NEW_PERMITS = 5000;

    public RemoteLowerTierEventStore(ClusterNode clusterNode, String internalToken,
                                     ChannelProvider channelProvider) {
        super(clusterNode, internalToken, channelProvider, null, null, null);
    }


    private LowerTierEventStoreServiceGrpc.LowerTierEventStoreServiceStub getLowerTierEventStoreStub() {
        Channel channel = channelProvider.get(clusterNode);
        if (channel == null) {
            throw new MessagingPlatformException(ErrorCode.NO_EVENTSTORE,
                                                 "No connection to lower tier event store available");
        }
        return LowerTierEventStoreServiceGrpc.newStub(channel).withInterceptors(
                new InternalTokenAddingInterceptor(internalToken));
    }

    /**
     * @param context the name of the context
     * @param from    the token of the first transaction to retrieve
     * @param to      the token of the last event to retrieve (exclusive)
     * @return flux of {@link TransactionWithToken} objects
     */
    @Override
    public Flux<TransactionWithToken> eventTransactions(String context, long from, long to) {
        return Flux.push(sink -> {
            AtomicReference<StreamObserver<GetTransactionsRequest>> requestStreamHolder = new AtomicReference<>();
            requestStreamHolder.set(
                    getLowerTierEventStoreStub().listEvents(
                            createResponseObserver(context, sink, requestStreamHolder)));
            requestStreamHolder.get().onNext(GetTransactionsRequest.newBuilder()
                                                                   .setNumberOfPermits(INITIAL_PERMITS)
                                                                   .setTrackingToken(from)
                                                                   .setToTrackingToken(to)
                                                                   .setContext(context)
                                                                   .build());
        });
    }

    /**
     * Creates a flux to read snapshot transactions from a lower tier event store.
     *
     * @param context the name of the context
     * @param from    the token of the first transaction to retrieve
     * @param to      the token of the last snapshot to retrieve (exclusive)
     * @return flux with snapshot transactions
     */
    @Override
    public Flux<TransactionWithToken> snapshotTransactions(String context, long from, long to) {
        return Flux.push(sink -> {
            AtomicReference<StreamObserver<GetTransactionsRequest>> requestStreamHolder = new AtomicReference<>();
            requestStreamHolder.set(
                    getLowerTierEventStoreStub().listSnapshots(
                            createResponseObserver(context, sink, requestStreamHolder)));
            requestStreamHolder.get().onNext(GetTransactionsRequest.newBuilder()
                                                                   .setNumberOfPermits(INITIAL_PERMITS)
                                                                   .setTrackingToken(from)
                                                                   .setToTrackingToken(to)
                                                                   .setContext(context)
                                                                   .build());
        });
    }

    @Nonnull
    private StreamObserver<TransactionWithToken> createResponseObserver(String context,
                                                                        FluxSink<TransactionWithToken> sink,
                                                                        AtomicReference<StreamObserver<GetTransactionsRequest>> requestStreamHolder) {
        return new StreamObserver<TransactionWithToken>() {
            final AtomicLong sendPermitsAfter = new AtomicLong(THRESHOLD);


            @Override
            public void onNext(
                    TransactionWithToken transactionWithToken) {
                sink.next(transactionWithToken);
                if (sendPermitsAfter.decrementAndGet() == 0) {
                    logger.debug("{}: Received token {}. Sending new permits", context,
                                 transactionWithToken.getToken());
                    requestStreamHolder.get().onNext(GetTransactionsRequest.newBuilder()
                                                                           .setNumberOfPermits(NEW_PERMITS)
                                                                           .build());
                    sendPermitsAfter.addAndGet(NEW_PERMITS);
                }
            }


            @Override
            public void onError(
                    Throwable throwable) {
                logger.warn("{}: Received error", context, throwable);
                StreamObserverUtils.complete(requestStreamHolder.get());
                sink.error(
                        throwable);
            }

            @Override
            public void onCompleted() {
                logger.warn("{}: Received completed", context);
                StreamObserverUtils.complete(requestStreamHolder.get());
                sink.complete();
            }
        };
    }

    /**
     * Get the highest sequence nr for an aggregate in a context. Sends additional information to the lower tier
     * event store to optimize the search process.
     *
     * @param context     the name of the context
     * @param aggregateId the identifier of the aggregate
     * @param maxSegments maximum number of segments to search for the aggregate (implementation may ignore this value)
     * @param maxToken    events with tokens higher than this token have already been checked for this aggregate
     * @return sequence number if found
     */
    @Override
    public Optional<Long> getHighestSequenceNr(String context, String aggregateId, int maxSegments, long maxToken) {
        CompletableFuture<GetHighestSequenceNumberResponse> responseCompletableFuture = new CompletableFuture<>();
        GetHighestSequenceNumberRequest request = GetHighestSequenceNumberRequest.newBuilder()
                                                                                 .setAggregateIdentifier(aggregateId)
                                                                                 .setContext(context)
                                                                                 .setMaxSegmentsHint(maxSegments)
                                                                                 .setMaxTokenHint(maxToken)
                                                                                 .build();
        getLowerTierEventStoreStub().getHighestSequenceNumber(request,
                                                              new CompletableStreamObserver<>(responseCompletableFuture,
                                                                                              "getHighestSequenceNumber",
                                                                                              logger));
        try {
            GetHighestSequenceNumberResponse response = responseCompletableFuture.get(5, TimeUnit.SECONDS);
            return response.getSequenceNumber() < 0 ? Optional.empty() : Optional.of(response.getSequenceNumber());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw MessagingPlatformException.create(e);
        } catch (ExecutionException executionException) {
            throw GrpcExceptionBuilder.parse(executionException.getCause());
        } catch (TimeoutException e) {
            throw MessagingPlatformException.create(e);
        }
    }

    /**
     * Retrieves the last event token in the context.
     *
     * @param context the name of the context
     * @return the last event token in the context
     */
    @Override
    public long getLastToken(String context) {
        CompletableFuture<TrackingToken> trackingTokenCompletableFuture = new CompletableFuture<>();
        GetLastTokenRequest request = GetLastTokenRequest.newBuilder()
                                                         .build();
        super.getLastToken(context,
                           request,
                           new CompletableStreamObserver<>(trackingTokenCompletableFuture, "getLastToken", logger));
        try {
            return trackingTokenCompletableFuture.get(5, TimeUnit.SECONDS).getToken();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw MessagingPlatformException.create(e);
        } catch (ExecutionException executionException) {
            throw MessagingPlatformException.create(executionException.getCause());
        } catch (TimeoutException e) {
            throw MessagingPlatformException.create(e);
        }
    }

    /**
     * Retrieves the last snapshot token in the context.
     *
     * @param context the name of the context
     * @return the last snapshot token in the context
     */
    @Override
    public long getLastSnapshotToken(String context) {

        CompletableFuture<TrackingToken> responseCompletableFuture = new CompletableFuture<>();
        GetLastSnapshotTokenRequest request = GetLastSnapshotTokenRequest.newBuilder()
                                                                         .setContext(context)
                                                                         .build();
        getLowerTierEventStoreStub().getLastSnapshotToken(request,
                                                          new CompletableStreamObserver<>(responseCompletableFuture,
                                                                                          "getHighestSequenceNumber",
                                                                                          logger));
        try {
            return responseCompletableFuture.get(5, TimeUnit.SECONDS).getToken();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw MessagingPlatformException.create(e);
        } catch (ExecutionException executionException) {
            throw GrpcExceptionBuilder.parse(executionException.getCause());
        } catch (TimeoutException e) {
            throw MessagingPlatformException.create(e);
        }
    }
}
