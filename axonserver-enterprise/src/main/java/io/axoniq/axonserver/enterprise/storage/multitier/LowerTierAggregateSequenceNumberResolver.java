package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.localstorage.file.RemoteAggregateSequenceNumberResolver;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Optional;
import javax.annotation.PostConstruct;

/**
 * Component that finds the highest sequence number for an aggregate on a lower tier node.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class LowerTierAggregateSequenceNumberResolver implements RemoteAggregateSequenceNumberResolver {

    private final ApplicationContext applicationContext;
    private LowerTierEventStoreLocator lowerTierEventStoreLocator;

    public LowerTierAggregateSequenceNumberResolver(
            ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void init() {
        lowerTierEventStoreLocator = applicationContext.getBean(LowerTierEventStoreLocator.class);
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String context, String aggregateId, int maxSegments, long maxToken) {
        if (maxToken >= 0 && lowerTierEventStoreLocator.hasLowerTier(context)) {
            return lowerTierEventStoreLocator
                    .getEventStore(context)
                    .getHighestSequenceNr(context, aggregateId, maxSegments, maxToken);
        }
        return Optional.empty();
    }
}
