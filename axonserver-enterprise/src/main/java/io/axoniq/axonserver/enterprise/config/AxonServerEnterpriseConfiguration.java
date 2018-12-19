package io.axoniq.axonserver.enterprise.config;

import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalanceStrategyController;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancingController;
import io.axoniq.axonserver.config.AxonServerFreeConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.ClusterMetricTarget;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.cluster.RaftGroupRepositoryManager;
import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import io.axoniq.axonserver.enterprise.cluster.raftfacade.RaftLoadBalanceStrategyControllerFacade;
import io.axoniq.axonserver.enterprise.cluster.raftfacade.RaftProcessorLoadBalancingControllerFacade;
import io.axoniq.axonserver.enterprise.messaging.query.MetricsBasedQueryHandlerSelector;
import io.axoniq.axonserver.enterprise.storage.file.ClusterTransactionManagerFactory;
import io.axoniq.axonserver.enterprise.storage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.enterprise.topology.ClusterTopology;
import io.axoniq.axonserver.enterprise.cluster.raftfacade.RaftUserControllerFacade;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.message.query.QueryHandlerSelector;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.rest.LoadBalanceStrategyControllerFacade;
import io.axoniq.axonserver.rest.ProcessorLoadBalancingControllerFacade;
import io.axoniq.axonserver.rest.UserControllerFacade;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.platform.user.UserController;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
            MessagingPlatformConfiguration messagingPlatformConfiguration,
            ClusterController clusterController, RaftLeaderProvider raftLeaderProvider, RaftGroupRepositoryManager raftGroupRepositoryManager,
            LifecycleController lifecycleController, LocalEventStore localEventStore) {
        return new EventStoreManager(messagingPlatformConfiguration, clusterController, lifecycleController, raftLeaderProvider, raftGroupRepositoryManager,
                                     localEventStore);
    }

    @Bean
    @Conditional(ClusteringAllowed.class)
    public MetricCollector metricCollector() {
        return new ClusterMetricTarget();
    }

    @Bean
    @Conditional(ClusteringAllowed.class)
    public Topology topology(ClusterController clusterController, GrpcRaftController grpcRaftController) {
        return new ClusterTopology(clusterController, grpcRaftController);
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
    public StorageTransactionManagerFactory storageTransactionManagerFactory(GrpcRaftController raftController) {
        return new ClusterTransactionManagerFactory(raftController);
    }

    @Bean
    @ConditionalOnMissingBean(UserControllerFacade.class)
    public UserControllerFacade userControllerFacade(UserController userController, RaftConfigServiceFactory raftServiceFactory) {
        return new RaftUserControllerFacade(userController, raftServiceFactory);
    }

    @Bean
    @ConditionalOnMissingBean(LoadBalanceStrategyControllerFacade.class)
    public LoadBalanceStrategyControllerFacade loadBalanceStrategyControllerFacade(
            LoadBalanceStrategyController controller, RaftConfigServiceFactory raftServiceFactory) {
        return new RaftLoadBalanceStrategyControllerFacade(controller, raftServiceFactory);
    }


    @Bean
    @ConditionalOnMissingBean(ProcessorLoadBalancingControllerFacade.class)
    public ProcessorLoadBalancingControllerFacade processorLoadBalancingControllerFacade(
            ProcessorLoadBalancingController processorLoadBalancingController, RaftConfigServiceFactory raftServiceFactory) {
        return new RaftProcessorLoadBalancingControllerFacade(processorLoadBalancingController, raftServiceFactory);
    }

}
