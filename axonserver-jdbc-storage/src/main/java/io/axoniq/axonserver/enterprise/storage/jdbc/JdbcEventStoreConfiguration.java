package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.config.AxonServerEnterpriseConfiguration;
import io.axoniq.axonserver.enterprise.storage.jdbc.multicontext.SchemaPerContextMultiContextStrategy;
import io.axoniq.axonserver.enterprise.storage.jdbc.multicontext.SingleSchemaMultiContextStrategy;
import io.axoniq.axonserver.enterprise.storage.jdbc.serializer.ProtoMetaDataSerializer;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.SQLException;

/**
 * Spring Boot autoconfiguration class to setup the event store on this node to store the events in a relational
 * database.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Configuration
@ConditionalOnProperty(name = "axoniq.axonserver.storage", havingValue = "jdbc")
@AutoConfigureBefore(AxonServerEnterpriseConfiguration.class)
public class JdbcEventStoreConfiguration {

    /**
     * Returns the JdbcEventStoreFactory as Spring bean eventStoreFactory.
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
        return new JdbcEventStoreFactory(storageProperties,
                                         storageTransactionManagerFactory,
                                         metaDataSerializer(),
                                         multiContextStrategy(storageProperties),
                                         leaderProvider);
    }

    /**
     * Determines how to setup multi-context support for this JDBC event store. Options supported are single-schema
     * (tables for all contexts are
     * stored in the same database schema, table name contains context), and schema-per-context (each context gets its
     * own schema).
     *
     * @param storageProperties storage properties for the event store
     * @return the strategy object
     */
    private MultiContextStrategy multiContextStrategy(StorageProperties storageProperties) {
        try {
            switch (storageProperties.getMultiContextStrategy()) {
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

    /**
     * Retrieves the serializer to use to serialize event metadata to the database. Currently only supports serializing
     * as protobuf object.
     *
     * @return the serializer
     */
    private MetaDataSerializer metaDataSerializer() {
        return new ProtoMetaDataSerializer();
    }
}
