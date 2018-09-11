package io.axoniq.axonserver.config;

import io.axoniq.axonserver.features.DefaultFeatureChecker;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.LowMemoryEventStoreFactory;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.message.query.QueryHandlerSelector;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.message.query.RoundRobinQueryHandlerSelector;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.topology.DefaultEventStoreLocator;
import io.axoniq.axonserver.topology.DefaultTopology;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Author: marc
 */
@Configuration
public class AxonServerFreeConfiguration {

    @Bean
    @ConditionalOnMissingBean(StorageTransactionManagerFactory.class)
    public StorageTransactionManagerFactory storageTransactionManagerFactory() {
        return new DefaultStorageTransactionManagerFactory();
    }

    @Bean
    @ConditionalOnMissingBean(EventTransformerFactory.class)
    public EventTransformerFactory eventTransformerFactory() {
        return new DefaultEventTransformerFactory();
    }

    @Bean
    @ConditionalOnMissingBean(EventStoreFactory.class)
    public EventStoreFactory eventStoreFactory(EmbeddedDBProperties embeddedDBProperties, EventTransformerFactory eventTransformerFactory,
                                               StorageTransactionManagerFactory storageTransactionManagerFactory) {
        return new LowMemoryEventStoreFactory(embeddedDBProperties, eventTransformerFactory, storageTransactionManagerFactory);
    }

    @Bean
    @ConditionalOnMissingBean(QueryHandlerSelector.class)
    public QueryHandlerSelector queryHandlerSelector() {
        return new RoundRobinQueryHandlerSelector();
    }

    @Bean
    @ConditionalOnMissingBean(Topology.class)
    public Topology topology(MessagingPlatformConfiguration configuration) {
        return new DefaultTopology(configuration);
    }

    @Bean
    @ConditionalOnMissingBean(MetricCollector.class)
    public MetricCollector metricCollector() {
        return new DefaultMetricCollector();
    }

    @Bean
    @ConditionalOnMissingBean(EventStoreLocator.class)
    public EventStoreLocator eventStoreLocator(LocalEventStore localEventStore) {
        return new DefaultEventStoreLocator(localEventStore);
    }

    @Bean
    @ConditionalOnMissingBean(FeatureChecker.class)
    public FeatureChecker featureChecker() {
        return new DefaultFeatureChecker();
    }

}
