package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.enterprise.cluster.manager.LeaderEventStoreLocator;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
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
import io.axoniq.axonserver.message.event.EventStore;
import io.axoniq.axonserver.queryparser.EventStoreQueryParser;
import io.axoniq.axonserver.queryparser.Query;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Controller;

import java.io.InputStream;
import java.text.ParseException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Version of the event store that is used when context is multi-tier.
 * Depending on the operation the request is forwarded to the leader, the current node or a secondary node.
 * Affected operations:
 * <ul>
 *     <li>listAggregateEvents - gets recent events from primary, if older needed merges with events from secondary node</li>
 *     <li>listSnapshotEvents - gets recent snapshot(s) from primary, if older needed merges with snapshot(s) from secondary node</li>
 *     <li>listEvents - if requested start token is available on primary nodes, read from current node or leader
 *     (depending on the allowReadingFromFollower setting), otherwise read from secondary node.
 *     </li>
 *     <li>searchEvents - if query is limited to timestamp available on primary nodes, read from current node or leader
 *     (depending on the allowReadingFromFollower setting), otherwise read from secondary node.  </li>
 *      <li>getTokenAt - if timestamp is available on primary nodes read from leader, otherwise read from secondary node</li>
 *      <li>readHighestSequenceNumber - get from leader, if not found check on secondary</li>
 *      <li>getFirstToken - read from secondary node</li>
 * </ul>
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Controller
public class MultiTierEventStore implements EventStore {

    private final LeaderEventStoreLocator leaderEventStoreLocator;
    private final MultiTierReaderEventStoreLocator multiTierReaderEventStoreLocator;
    private final LowerTierEventStoreLocator lowerTierEventStoreLocator;

    /**
     * Constructor for the {@link MultiTierEventStore}.
     *
     * @param leaderEventStoreLocator          provides an event store facade for the leader
     * @param multiTierReaderEventStoreLocator provides an event store facade to read event stream
     * @param lowerTierEventStoreLocator       provides an event store facade to a lower tier
     */
    public MultiTierEventStore(LeaderEventStoreLocator leaderEventStoreLocator,
                               MultiTierReaderEventStoreLocator multiTierReaderEventStoreLocator,
                               LowerTierEventStoreLocator lowerTierEventStoreLocator) {
        this.leaderEventStoreLocator = leaderEventStoreLocator;
        this.multiTierReaderEventStoreLocator = multiTierReaderEventStoreLocator;
        this.lowerTierEventStoreLocator = lowerTierEventStoreLocator;
    }

    /**
     * Stores a snapshot in the event store. This request is always sent to the leader of the context.
     *
     * @param context  the context where the snapshot are stored
     * @param snapshot the snapshot
     * @return completable future that completes when snapshot is stored on leader
     */
    @Override
    public CompletableFuture<Confirmation> appendSnapshot(String context, Event snapshot) {
        return leaderEventStoreLocator.getEventStore(context).appendSnapshot(context, snapshot);
    }

    /**
     * Creates a connection that receives events to be stored in a single transaction. The request is always sent to
     * the leader of the context.
     *
     * @param context          the context where the events are stored
     * @param responseObserver response stream where the event store can confirm completion of the transaction
     * @return stream to send events to
     */
    @Override
    public StreamObserver<InputStream> createAppendEventConnection(String context,
                                                                   StreamObserver<Confirmation> responseObserver) {
        return leaderEventStoreLocator.getEventStore(context).createAppendEventConnection(context, responseObserver);
    }

    /**
     * Read events for an aggregate. Reads from the leader of the context and if the leader no longer has all the
     * requested events for the aggregate it will also read from a lower tier.
     *
     * @param context                the context to read from
     * @param request                the request containing the aggregate identifier and read options
     * @param responseStreamObserver {@link StreamObserver} where the events will be published
     */
    @Override
    public void listAggregateEvents(String context, GetAggregateEventsRequest request,
                                    StreamObserver<SerializedEvent> responseStreamObserver) {
        leaderEventStoreLocator.getEventStore(context)
                               .listAggregateEvents(context,
                                                    request,
                                                    new MultiTierReadAggregateStreamObserver(
                                                            context,
                                                            request,
                                                            responseStreamObserver,
                                                            lowerTierEventStoreLocator::getEventStore));
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
    @Override
    public StreamObserver<GetEventsRequest> listEvents(String context,
                                                       StreamObserver<InputStream> responseStreamObserver) {
        return new StreamObserver<GetEventsRequest>() {
            private final AtomicReference<StreamObserver<GetEventsRequest>> eventStoreStreamObserver = new AtomicReference<>();

            @Override
            public void onNext(GetEventsRequest request) {
                if (eventStoreStreamObserver.get() == null) {
                    try {
                        EventStore eventStore = multiTierReaderEventStoreLocator.getEventStoreWithToken(
                                context,
                                request.getForceReadFromLeader(),
                                request.getTrackingToken());
                        eventStoreStreamObserver.set(eventStore.listEvents(context, responseStreamObserver));
                    } catch (MessagingPlatformException mpe) {
                        responseStreamObserver.onError(GrpcExceptionBuilder.build(mpe));
                        return;
                    }
                }
                eventStoreStreamObserver.get().onNext(request);
            }

            @Override
            public void onError(Throwable throwable) {
                Optional.ofNullable(eventStoreStreamObserver.get()).ifPresent(s -> s.onError(throwable));
            }

            @Override
            public void onCompleted() {
                Optional.ofNullable(eventStoreStreamObserver.get()).ifPresent(StreamObserver::onCompleted);
                StreamObserverUtils.complete(responseStreamObserver);
            }
        };
    }

    /**
     * Returns the first token in the event store. This will get the token from the lower tier.
     *
     * @param context          the name of the context
     * @param request          request from client
     * @param responseObserver {@link StreamObserver} where result will be send to
     */
    @Override
    public void getFirstToken(String context, GetFirstTokenRequest request,
                              StreamObserver<TrackingToken> responseObserver) {
        lowerTierEventStoreLocator.getEventStore(context)
                                  .getFirstToken(context, request, responseObserver);
    }

    /**
     * Returns the last token in the event store.
     *
     * @param context          the name of the context
     * @param request          request from client
     * @param responseObserver {@link StreamObserver} where result will be send to
     */
    @Override
    public void getLastToken(String context, GetLastTokenRequest request,
                             StreamObserver<TrackingToken> responseObserver) {
        leaderEventStoreLocator.getEventStore(context)
                               .getLastToken(context, request, responseObserver);
    }

    /**
     * Returns the token in the event store for a specific time. If the time is less than the retention time
     * it retrieves the token from the leader, otherwise from a lower tier server.
     *
     * @param context          the name of the context
     * @param request          request from client
     * @param responseObserver {@link StreamObserver} where result will be send to
     */
    @Override
    public void getTokenAt(String context, GetTokenAtRequest request, StreamObserver<TrackingToken> responseObserver) {
        multiTierReaderEventStoreLocator.getEventStoreFromTimestamp(context, false, request.getInstant())
                                        .getTokenAt(context, request, responseObserver);
    }

    /**
     * Finds the highest sequence number for an aggregate. First checks in leader, if not found on leader it checks
     * on the lower tier.
     *
     * @param context          the name of the context
     * @param request          contains the aggregate id
     * @param responseObserver {@link StreamObserver} where result will be send to
     */
    @Override
    public void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
                                      StreamObserver<ReadHighestSequenceNrResponse> responseObserver) {
        leaderEventStoreLocator.getEventStore(context).readHighestSequenceNr(
                context,
                request,
                new StreamObserver<ReadHighestSequenceNrResponse>() {
                    private final AtomicBoolean forwarded = new AtomicBoolean();

                    @Override
                    public void onNext(
                            ReadHighestSequenceNrResponse response) {
                        if (response.getToSequenceNr() < 0) {
                            forwarded.set(true);
                            lowerTierEventStoreLocator.getEventStore(context).readHighestSequenceNr(context,
                                                                                                    request,
                                                                                                    responseObserver);
                        } else {
                            responseObserver.onNext(response);
                        }
                    }

                    @Override
                    public void onError(
                            Throwable throwable) {
                        if (!forwarded.get()) {
                            responseObserver.onError(throwable);
                        }
                    }

                    @Override
                    public void onCompleted() {
                        if (!forwarded.get()) {
                            responseObserver.onCompleted();
                        }
                    }
                });
    }

    /**
     * Handles a bi-directional stream to execute ad-hoc queries. The first request sent on the stream contains the
     * query and a number of permits. The caller can send additional messages on the request stream to provide
     * additional
     * permits. Results are retrieved from the current node/leader if the query is time constraint and the data
     * for the time period is available in the primary nodes, otherwise it will return data from the lower tier.
     *
     * @param context          the name of the context
     * @param responseObserver {@link StreamObserver} where results will be published
     * @return StreamObserver to send the initial query and additional permits
     */
    @Override
    public StreamObserver<QueryEventsRequest> queryEvents(String context,
                                                          StreamObserver<QueryEventsResponse> responseObserver) {
        return new StreamObserver<QueryEventsRequest>() {
            private final AtomicReference<StreamObserver<QueryEventsRequest>> eventStoreStreamObserver = new AtomicReference<>();

            @Override
            public void onNext(QueryEventsRequest queryEventsRequest) {
                if (eventStoreStreamObserver.get() == null) {
                    try {
                        EventStore eventStore;
                        if (queryEventsRequest.getQuery().isEmpty()) {
                            eventStore = leaderEventStoreLocator.getEventStore(context);
                        } else {
                            Query query = new EventStoreQueryParser().parse(
                                    queryEventsRequest.getQuery());
                            if (query.getStartTime() == 0) {
                                eventStore = multiTierReaderEventStoreLocator.getEventStoreWithToken(
                                        context,
                                        queryEventsRequest
                                                .getForceReadFromLeader(),
                                        0);
                            } else {
                                eventStore = multiTierReaderEventStoreLocator.getEventStoreFromTimestamp(
                                        context,
                                        queryEventsRequest
                                                .getForceReadFromLeader(),
                                        query.getStartTime());
                            }
                        }
                        eventStoreStreamObserver.set(eventStore.queryEvents(context, responseObserver));
                    } catch (ParseException e) {
                        responseObserver.onError(new MessagingPlatformException(ErrorCode.INVALID_QUERY,
                                                                                e.getMessage()));
                    }
                }
                eventStoreStreamObserver.get().onNext(queryEventsRequest);
            }

            @Override
            public void onError(Throwable throwable) {
                Optional.ofNullable(eventStoreStreamObserver.get()).ifPresent(StreamObserver::onCompleted);
            }

            @Override
            public void onCompleted() {
                Optional.ofNullable(eventStoreStreamObserver.get()).ifPresent(StreamObserver::onCompleted);
                StreamObserverUtils.complete(responseObserver);
            }
        };
    }

    /**
     * Returns snapshots for a specific aggregate where the sequence number is in given range.
     *
     * @param context          the name of the context
     * @param request          contains the aggregate id and optionally range of sequence numbers to search for
     * @param responseObserver {@link StreamObserver} where the snapshots will be sent to
     */
    @Override
    public void listAggregateSnapshots(String context, GetAggregateSnapshotsRequest request,
                                       StreamObserver<SerializedEvent> responseObserver) {
        leaderEventStoreLocator.getEventStore(context)
                               .listAggregateSnapshots(context,
                                                       request,
                                                       new MultiTierReadSnapshotStreamObserver(context,
                                                                                               request,
                                                                                               responseObserver,
                                                                                               lowerTierEventStoreLocator::getEventStore));
    }

    /**
     * Deletes all data for the given context from the event store.
     * @param context the name of the context
     */
    @Override
    public void deleteAllEventData(String context) {
        leaderEventStoreLocator.getEventStore(context).deleteAllEventData(context);
    }

}
