package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.enterprise.replication.RaftGroupRepositoryManager;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.metric.MetricName;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Component;

/**
 * Provides information on the multi-tier configuration of the context.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class MultiTierInformationProvider {

    private final RaftGroupRepositoryManager raftGroupRepositoryManager;
    private final MetricCollector metricCollector;

    /**
     * Instantiates a {@link MultiTierInformationProvider}.
     * @param raftGroupRepositoryManager provides information on the context configuration
     * @param metricCollector provides runtime data on remote nodes
     */
    public MultiTierInformationProvider(
            RaftGroupRepositoryManager raftGroupRepositoryManager,
            MetricCollector metricCollector
    ) {
        this.raftGroupRepositoryManager = raftGroupRepositoryManager;
        this.metricCollector = metricCollector;
    }

    /**
     * Checks if there is a lower level tier for the given {@code context}, based on the role of the current node
     * in the context.
     * @param context the context to check
     * @return true if there is a next tier
     */
    public boolean isMultiTier(String context) {
        return raftGroupRepositoryManager.hasLowerTier(context);
    }

    /**
     * Returns the tier number of the current node in the given {@code context}. Primary nodes are tier 0.
     * @param context the context to check
     * @return the tier number of the current node in the context
     */
    public int tier(String context) {
        return raftGroupRepositoryManager.tier(context);
    }

    /**
     * Returns the minimum token available on all nodes on the next tier.
     * @param context the context to check
     * @param metricName  the metric name containing the last token for snapshot or event
     * @return the minimum token available on all nodes
     */
    public long safeToken(String context, MetricName metricName) {
        return raftGroupRepositoryManager.nextTierEventStores(context)
                                         .stream()
                                         .map(node -> lastEventToken(node, context, metricName))
                                         .min(Long::compareTo).orElse(-1L);
    }

    private long lastEventToken(String node, String context, MetricName metricName) {
        return (long) metricCollector.apply(metricName.metric(), Tags
                .of(MeterFactory.CONTEXT, context, "axonserver", node)).value();
    }
}
