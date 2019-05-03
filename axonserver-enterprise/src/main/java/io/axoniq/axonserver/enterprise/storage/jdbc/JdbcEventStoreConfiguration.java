package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.config.AxonServerEnterpriseConfiguration;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.SQLException;

/**
 * @author Marc Gathier
 */
@Configuration
@ConditionalOnProperty(name = "axoniq.axonserver.storage", havingValue = "jdbc")
@AutoConfigureBefore(AxonServerEnterpriseConfiguration.class)
public class JdbcEventStoreConfiguration {

    @Bean
    public EventStoreFactory eventStoreFactory(StorageProperties storageProperties,
                                               StorageTransactionManagerFactory storageTransactionManagerFactory,
                                               RaftLeaderProvider raftLeaderProvider) {
        return new JdbcEventStoreFactory(storageProperties, storageTransactionManagerFactory, metaDataSerializer(), multiContextStrategy(storageProperties), syncStrategy(storageProperties, raftLeaderProvider));
    }

    private MultiContextStrategy multiContextStrategy(StorageProperties storageProperties) {
        try {
            switch (storageProperties.getMultiContextValue()) {
                case SINGLE_SCHEMA:
                        return new SingleSchemaMultiContextStrategy(storageProperties.getVendorSpecific());
                case SCHEMA_PER_CONTEXT:
                    return new SchemaPerContextMultiContextStrategy(storageProperties.getVendorSpecific());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Missing multi-context configuration property");
    }

    private MetaDataSerializer metaDataSerializer() {
        return new ProtoMetaDataSerializer();
    }

    private SyncStrategy syncStrategy(StorageProperties storageProperties, RaftLeaderProvider raftLeaderProvider) {
        if( storageProperties.isStoreOnLeaderOnly()) {
            return new StoreOnLeaderSyncStrategy(raftLeaderProvider::isLeader);
        }
        return new StoreAlwaysSyncStrategy();
    }


}
