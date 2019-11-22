package io.axoniq.axonserver.enterprise.config;

import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.access.user.UserControllerFacade;
import io.axoniq.axonserver.config.AxonServerStandardConfiguration;
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
import io.axoniq.axonserver.enterprise.cluster.raftfacade.RaftUserControllerFacade;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyController;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ProcessorLoadBalancingService;
import io.axoniq.axonserver.enterprise.messaging.query.MetricsBasedQueryHandlerSelector;
import io.axoniq.axonserver.enterprise.storage.file.ClusterTransactionManagerFactory;
import io.axoniq.axonserver.enterprise.storage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.grpc.ChannelProvider;
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
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * @author Marc Gathier
 */
@Configuration
@AutoConfigureBefore(AxonServerStandardConfiguration.class)
public class AxonServerEnterpriseConfiguration {

    @Bean
    public EventStoreManager eventStoreManager(
            MessagingPlatformConfiguration messagingPlatformConfiguration,
            ClusterController clusterController, RaftLeaderProvider raftLeaderProvider, RaftGroupRepositoryManager raftGroupRepositoryManager,
            LifecycleController lifecycleController, LocalEventStore localEventStore,
            ChannelProvider channelProvider) {
        return new EventStoreManager(messagingPlatformConfiguration, clusterController, lifecycleController, raftLeaderProvider, raftGroupRepositoryManager,
                                     localEventStore,
                                     channelProvider);
    }

    @Bean
    public MetricCollector metricCollector() {
        return new ClusterMetricTarget();
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
    public StorageTransactionManagerFactory storageTransactionManagerFactory(GrpcRaftController raftController, MessagingPlatformConfiguration messagingPlatformConfiguration) {
        return new ClusterTransactionManagerFactory(raftController, messagingPlatformConfiguration);
    }

    @Bean
    @ConditionalOnMissingBean(UserControllerFacade.class)
    public UserControllerFacade userControllerFacade(UserController userController, PasswordEncoder passwordEncoder, RaftConfigServiceFactory raftServiceFactory) {
        return new RaftUserControllerFacade(userController, passwordEncoder, raftServiceFactory);
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
            ProcessorLoadBalancingService processorLoadBalancingController,
            RaftConfigServiceFactory raftServiceFactory) {
        return new RaftProcessorLoadBalancingControllerFacade(processorLoadBalancingController, raftServiceFactory);
    }

}
