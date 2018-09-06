package io.axoniq.axonhub.localstorage.jdbc;

import io.axoniq.axonhub.localstorage.EventStore;
import io.axoniq.axonhub.localstorage.EventStoreFactory;
import io.axoniq.axonhub.localstorage.EventType;
import io.axoniq.axonhub.localstorage.EventTypeContext;
import io.axoniq.axonhub.localstorage.transaction.SingleInstanceTransactionManager;
import io.axoniq.axonhub.localstorage.transaction.StorageTransactionManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@ConditionalOnProperty(name = "axoniq.axonhub.storage", havingValue = "jdbc")
@Component
public class JdbcEventStoreFactory implements EventStoreFactory {


    private final StorageProperties storageProperties;

    public JdbcEventStoreFactory(StorageProperties storageProperties) {
        this.storageProperties = storageProperties;
    }

    @Override
    public EventStore createEventManagerChain(String context) {
        return new JdbcEventStore(new EventTypeContext(context, EventType.EVENT), storageProperties.dataSource());
    }

    @Override
    public EventStore createSnapshotManagerChain(String context) {
        return new JdbcSnapshotStore(new EventTypeContext(context, EventType.SNAPSHOT), storageProperties.dataSource());
    }

    @Override
    public StorageTransactionManager createTransactionManager(EventStore datafileManagerChain) {
        return new SingleInstanceTransactionManager(datafileManagerChain);
    }
}
