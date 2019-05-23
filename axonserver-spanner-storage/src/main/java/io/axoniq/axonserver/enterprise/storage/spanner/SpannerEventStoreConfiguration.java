package io.axoniq.axonserver.enterprise.storage.spanner;

import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.config.AxonServerEnterpriseConfiguration;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * Spring Boot autoconfiguration class to setup the event store on this node to store the events in a Google spanner
 * database.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Configuration
@ConditionalOnProperty(name = "axoniq.axonserver.storage", havingValue = "spanner")
@AutoConfigureBefore(AxonServerEnterpriseConfiguration.class)
public class SpannerEventStoreConfiguration {

    /**
     * Returns the SpannerEventStoreFactory as Spring bean eventStoreFactory.
     *
     * @param storageProperties                specific storage properties for the JDBC event store
     * @param storageTransactionManagerFactory the transaction manager to use
     * @param leaderProvider                   a leader provider that returns the leader node for a specific context
     * @return the eventStore factory
     */
    @Bean
    public EventStoreFactory eventStoreFactory(StorageProperties storageProperties,
                                               StorageTransactionManagerFactory storageTransactionManagerFactory,
                                               RaftLeaderProvider leaderProvider) {
        return new SpannerEventStoreFactory(storageProperties,
                                            storageTransactionManagerFactory,
                                            leaderProvider);
    }
}
