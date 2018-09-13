package io.axoniq.axonserver.enterprise.config;

import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.config.AxonServerFreeConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.ClusterMetricTarget;
import io.axoniq.axonserver.enterprise.cluster.internal.StubFactory;
import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.messaging.query.MetricsBasedQueryHandlerSelector;
import io.axoniq.axonserver.enterprise.storage.file.ClusterTransactionManagerFactory;
import io.axoniq.axonserver.enterprise.storage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.enterprise.storage.transaction.ReplicationManager;
import io.axoniq.axonserver.enterprise.topology.ClusterTopology;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.message.query.QueryHandlerSelector;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Author: marc
 */
@Configuration
@AutoConfigureBefore(AxonServerFreeConfiguration.class)
public class AxonServerEnterpriseConfiguration {

    @Bean
    @Conditional(ClusteringAllowed.class)
    public EventStoreManager eventStoreManager(
            ContextController contextController,
            MessagingPlatformConfiguration messagingPlatformConfiguration,
            StubFactory stubfactory, ClusterController clusterController,
            LifecycleController lifecycleController, LocalEventStore localEventStore,
            ApplicationEventPublisher applicationEventPublisher) {
        return new EventStoreManager(contextController, messagingPlatformConfiguration, stubfactory, clusterController, lifecycleController, localEventStore, applicationEventPublisher);
    }

    @Bean
    @Conditional(ClusteringAllowed.class)
    public MetricCollector metricCollector() {
        return new ClusterMetricTarget();
    }

    @Bean
    @Conditional(ClusteringAllowed.class)
    public Topology topology(ClusterController clusterController) {
        return new ClusterTopology(clusterController);
    }

    @Bean
    public QueryHandlerSelector queryHandlerSelector(
            QueryMetricsRegistry metricsRegistry) {
        return new MetricsBasedQueryHandlerSelector(metricsRegistry);
    }

    @Bean
    @ConditionalOnMissingBean(EventStoreFactory.class)
    @Conditional(MemoryMappedStorage.class)
    public EventStoreFactory eventStoreFactory(EmbeddedDBProperties embeddedDBProperties, EventTransformerFactory eventTransformerFactory,
                                               StorageTransactionManagerFactory storageTransactionManagerFactory) {
        return new DatafileEventStoreFactory(embeddedDBProperties, eventTransformerFactory, storageTransactionManagerFactory);
    }

    @Bean
    @ConditionalOnMissingBean(StorageTransactionManagerFactory.class)
    @Conditional(ClusteringAllowed.class)
    public StorageTransactionManagerFactory storageTransactionManagerFactory(ReplicationManager replicationManager) {
        return new ClusterTransactionManagerFactory(replicationManager);
    }

}
