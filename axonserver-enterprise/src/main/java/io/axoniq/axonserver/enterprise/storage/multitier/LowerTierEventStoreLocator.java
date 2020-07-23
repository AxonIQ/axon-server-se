package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.messaging.event.LowerTierEventStore;
import io.axoniq.axonserver.enterprise.replication.RaftGroupRepositoryManager;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricCollector;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Controller;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Component that can find the EventStore component to send requests to an event store at a lower tier than the current
 * tier (for a primary node it will find a secondary node).
 * <p>
 * Return the most up-to-date instance in that tier.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Controller
public class LowerTierEventStoreLocator {

    private final RaftGroupRepositoryManager raftGroupRepositoryManager;
    private final MetricCollector metricCollector;
    private final LowerTierEventStoreFactory remoteEventStoreFactory;
    private final ClusterController clusterController;

    /**
     * Instantiates the controller.
     *
     * @param raftGroupRepositoryManager     provides information about the raft group/cntext
     * @param metricCollector                provides status information on the remote nodes
     * @param remoteEventStoreFactory        factory to create a proxy to a remote event store
     * @param clusterController              maintains status of remote nodes
     */
    public LowerTierEventStoreLocator(
            RaftGroupRepositoryManager raftGroupRepositoryManager,
            MetricCollector metricCollector,
            LowerTierEventStoreFactory remoteEventStoreFactory,
            ClusterController clusterController) {
        this.raftGroupRepositoryManager = raftGroupRepositoryManager;
        this.metricCollector = metricCollector;
        this.remoteEventStoreFactory = remoteEventStoreFactory;
        this.clusterController = clusterController;
    }


    /**
     * Returns the best event store to use to access the lower tier.
     * Throws a {@link MessagingPlatformException} if no event store at the next tier can be found.
     *
     * @param context the context for which to find the event store
     * @return the event store component to use
     */
    public LowerTierEventStore getEventStore(String context) {
        Set<String> nextTierEventStores = raftGroupRepositoryManager.nextTierEventStores(context);
        if (nextTierEventStores.isEmpty()) {
            throw new MessagingPlatformException(ErrorCode.NO_EVENTSTORE, "No event stores found at lower tier");
        }

        String node = selectNextTierNode(nextTierEventStores, context);
        return remoteEventStoreFactory.create(node);
    }

    private String selectNextTierNode(Set<String> nextTierEventStores, String context) {
        HashMap<String, Long> lastEventMap = new HashMap<>();
        nextTierEventStores.forEach(s -> {
            if (clusterController.isActive(s)) {
                lastEventMap.put(s, lastEvent(s, context));
            }
        });
        return lastEventMap.entrySet()
                           .stream()
                           .max(Map.Entry.comparingByValue())
                           .map(Map.Entry::getKey)
                           .orElseThrow(() -> new MessagingPlatformException(ErrorCode.NO_EVENTSTORE,
                                                                             "No event stores found at lower tier"));
    }

    private long lastEvent(String node, String context) {
        return (long) metricCollector.apply(BaseMetricName.AXON_EVENT_LAST_TOKEN.metric(),
                                            Tags.of(MeterFactory.CONTEXT, context, "axonserver", node)).value();
    }


    /**
     * Checks if the there are nodes in a lower tier for this context.
     *
     * @param context the name of the context
     * @return true if there are nodes in a lower tier for this context
     */
    public boolean hasLowerTier(String context) {
        Set<String> nextTierEventStores = raftGroupRepositoryManager.nextTierEventStores(context);
        return !nextTierEventStores.isEmpty();
    }
}
