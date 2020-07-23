package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.enterprise.cluster.CompletableStreamObserver;
import io.axoniq.axonserver.enterprise.cluster.manager.LeaderEventStoreLocator;
import io.axoniq.axonserver.enterprise.replication.RaftGroupRepositoryManager;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.GetFirstTokenRequest;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.PrimaryEventStore;
import io.axoniq.axonserver.message.event.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Component that returns an event store object to send requests for reading event streams in a multi-tier context.
 * Event streams include streams for tracking event processors and streams for ad-hoc queries on the event store.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Controller
public class MultiTierReaderEventStoreLocator {

    private final Logger logger = LoggerFactory.getLogger(PrimaryEventStore.class);
    private final RaftGroupRepositoryManager raftGroupRepositoryManager;
    private final LowerTierEventStoreLocator lowerTierEventStoreLocator;
    private final LeaderEventStoreLocator leaderEventStoreLocator;
    private final EmbeddedDBProperties embeddedDBProperties;
    private final Clock clock;
    private final LocalEventStore localEventStore;

    /**
     * Constructor for the locator.
     *
     * @param raftGroupRepositoryManager provides information on context definitions
     * @param lowerTierEventStoreLocator finds event store facade for lower tier node
     * @param leaderEventStoreLocator    finds event store facade for leader
     * @param embeddedDBProperties       provides information on retention times for primary tier
     * @param clock                      a clock to provide current time
     * @param localEventStore            the local event store
     */
    public MultiTierReaderEventStoreLocator(
            RaftGroupRepositoryManager raftGroupRepositoryManager,
            LowerTierEventStoreLocator lowerTierEventStoreLocator,
            LeaderEventStoreLocator leaderEventStoreLocator,
            EmbeddedDBProperties embeddedDBProperties,
            Clock clock,
            LocalEventStore localEventStore) {
        this.raftGroupRepositoryManager = raftGroupRepositoryManager;
        this.lowerTierEventStoreLocator = lowerTierEventStoreLocator;
        this.leaderEventStoreLocator = leaderEventStoreLocator;
        this.embeddedDBProperties = embeddedDBProperties;
        this.clock = clock;
        this.localEventStore = localEventStore;
    }


    /**
     * Finds the {@link EventStore} facade for the event store node to use to serve the request based on the first token
     * requested. For multi-tier storage the requested token may not be on the primary nodes anymore, so the request
     * should be handled by a lower-tier node.
     *
     * @param context                the context to find the leader for
     * @param forceReadingFromLeader is it forced to serve the response from the leader
     * @param firstToken             the first token to return
     * @return the EventStore facade for the leader
     */
    public EventStore getEventStoreWithToken(String context, boolean forceReadingFromLeader, long firstToken) {
        if (!forceReadingFromLeader
                && raftGroupRepositoryManager.containsStorageContext(context)
                && localEventStore.firstToken(context) <= firstToken) {
            return localEventStore;
        }

        if (forceReadingFromLeader) {
            EventStore leaderEventStore = leaderEventStoreLocator.getEventStore(context);
            if (getFirstToken(leaderEventStore, context) <= firstToken) {
                return leaderEventStore;
            }
        }

        return lowerTierEventStoreLocator.getEventStore(context);
    }

    /**
     * Finds the {@link EventStore} facade for the event store node to use to serve the request based on the first
     * timestamp
     * requested. For multi-tier storage the requested timestamp may not be on the primary nodes anymore, so the
     * request
     * should be handled by a lower-tier node.
     *
     * @param context                the context to find the leader for
     * @param forceReadingFromLeader is it forced to serve the response from the leader
     * @param firstTimestamp         the first timestamp
     * @return the EventStore facade for the leader
     */
    public EventStore getEventStoreFromTimestamp(String context, boolean forceReadingFromLeader,
                                                 long firstTimestamp) {
        if (clock.millis() - embeddedDBProperties.getEvent().getRetentionTime(0) < firstTimestamp) {
            if (!forceReadingFromLeader
                    && raftGroupRepositoryManager.containsStorageContext(context)) {
                return localEventStore;
            } else {
                return leaderEventStoreLocator.getEventStore(context);
            }
        }
        return lowerTierEventStoreLocator.getEventStore(context);
    }

    private long getFirstToken(EventStore eventStore, String context) {
        CompletableFuture<TrackingToken> completableFuture = new CompletableFuture<>();
        eventStore.getFirstToken(context,
                                 GetFirstTokenRequest.newBuilder().build(),
                                 new CompletableStreamObserver<>(completableFuture, "getFirstToken", logger));
        try {
            return completableFuture.get().getToken();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.INTERRUPTED,
                                                 "Interrupted while retrieving first token for " + context);
        } catch (ExecutionException e) {
            throw MessagingPlatformException.create(e.getCause());
        }
    }
}
