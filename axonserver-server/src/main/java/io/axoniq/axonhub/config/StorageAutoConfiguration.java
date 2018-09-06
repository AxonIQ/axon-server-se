package io.axoniq.axonhub.config;

import io.axoniq.axonhub.cluster.ClusterController;
import io.axoniq.axonhub.licensing.Limits;
import io.axoniq.axonhub.localstorage.EventStoreFactory;
import io.axoniq.axonhub.localstorage.file.DatafileEventStoreFactory;
import io.axoniq.axonhub.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonhub.localstorage.file.LowMemoryEventStoreFactory;
import io.axoniq.axonhub.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonhub.localstorage.transaction.ReplicationManager;
import io.axoniq.axonhub.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonhub.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonhub.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonhub.rest.Feature;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Author: marc
 */
@Configuration
public class StorageAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(StorageTransactionManagerFactory.class)
    public StorageTransactionManagerFactory storageTransactionManagerFactory(ClusterController clusterController, ReplicationManager replicationManager) {
        return new DefaultStorageTransactionManagerFactory(clusterController.isClustered(), replicationManager);
    }

    @Bean
    @ConditionalOnMissingBean(EventTransformerFactory.class)
    public EventTransformerFactory eventTransformerFactory() {
        return new DefaultEventTransformerFactory();
    }

    @Bean
    @ConditionalOnMissingBean(EventStoreFactory.class)
    public EventStoreFactory eventStoreFactory(EmbeddedDBProperties embeddedDBProperties, EventTransformerFactory eventTransformerFactory,
                                               StorageTransactionManagerFactory storageTransactionManagerFactory,
                                               Limits limits) {
        if(Feature.FILESYSTEM_STORAGE_ENGINE.enabled(limits)) {
            return new LowMemoryEventStoreFactory(embeddedDBProperties, eventTransformerFactory, storageTransactionManagerFactory);
        }
        return new DatafileEventStoreFactory(embeddedDBProperties, eventTransformerFactory, storageTransactionManagerFactory);
    }


}
