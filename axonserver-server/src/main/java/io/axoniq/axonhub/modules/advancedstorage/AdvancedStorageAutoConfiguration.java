package io.axoniq.axonhub.modules.advancedstorage;

import io.axoniq.axonhub.localstorage.EventStoreFactory;
import io.axoniq.axonhub.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonhub.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonhub.localstorage.transformation.EventTransformerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Author: marc
 */
@Configuration
public class AdvancedStorageAutoConfiguration {
    @Bean
    @Conditional(MultitierStorageCondition.class)
    public EventStoreFactory eventStoreFactory(EmbeddedDBProperties embeddedDBProperties, EventTransformerFactory eventTransformerFactory,
                                               StorageTransactionManagerFactory storageTransactionManagerFactory,AdvancedStorageProperties advancedStorageProperties) {
        return new MultitierDatafileEventStoreFactory(embeddedDBProperties, eventTransformerFactory, storageTransactionManagerFactory, advancedStorageProperties);
    }


}
