package io.axoniq.axonserver.enterprise.config;

import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.access.user.UserControllerFacade;
import io.axoniq.axonserver.config.AxonServerStandardConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterMetricTarget;
import io.axoniq.axonserver.enterprise.cluster.raftfacade.RaftLoadBalanceStrategyControllerFacade;
import io.axoniq.axonserver.enterprise.cluster.raftfacade.RaftProcessorLoadBalancingControllerFacade;
import io.axoniq.axonserver.enterprise.cluster.raftfacade.RaftUserControllerFacade;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.AdminProcessorLoadBalancingService;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyController;
import io.axoniq.axonserver.enterprise.messaging.query.MetricsBasedQueryHandlerSelector;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.storage.file.ClusterTransactionManagerFactory;
import io.axoniq.axonserver.enterprise.storage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.enterprise.storage.file.DefaultMultiContextEventTransformerFactory;
import io.axoniq.axonserver.enterprise.storage.file.EmbeddedDBPropertiesProvider;
import io.axoniq.axonserver.enterprise.storage.file.MultiContextEventTransformerFactory;
import io.axoniq.axonserver.enterprise.storage.multitier.LowerTierAggregateSequenceNumberResolver;
import io.axoniq.axonserver.enterprise.storage.multitier.MultiTierInformationProvider;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.message.query.QueryHandlerSelector;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.rest.LoadBalanceStrategyControllerFacade;
import io.axoniq.axonserver.rest.ProcessorLoadBalancingControllerFacade;
import io.axoniq.axonserver.taskscheduler.ScheduledTaskExecutor;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.TaskPayloadSerializer;
import io.axoniq.axonserver.taskscheduler.TaskRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Marc Gathier
 */
@Configuration
@AutoConfigureBefore(AxonServerStandardConfiguration.class)
public class AxonServerEnterpriseConfiguration {

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
    @ConditionalOnMissingBean(MultiContextEventTransformerFactory.class)
    public MultiContextEventTransformerFactory multiContextEventTransformerFactory(
            EventTransformerFactory eventTransformerFactory) {
        return new DefaultMultiContextEventTransformerFactory(eventTransformerFactory);
    }


    @Bean
    @ConditionalOnMissingBean(EventStoreFactory.class)
    public EventStoreFactory eventStoreFactory(EmbeddedDBPropertiesProvider embeddedDBPropertiesProvider,
                                               MultiContextEventTransformerFactory eventTransformerFactory,
                                               MultiTierInformationProvider secondaryEventStoreInformation,
                                               LowerTierAggregateSequenceNumberResolver lowerTierAggregateSequenceNumberResolver,
                                               MeterFactory meterFactory) {
        return new DatafileEventStoreFactory(embeddedDBPropertiesProvider,
                                             eventTransformerFactory,
                                             secondaryEventStoreInformation,
                                             lowerTierAggregateSequenceNumberResolver,
                                             meterFactory);
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
            LoadBalanceStrategyController controller) {
        return new RaftLoadBalanceStrategyControllerFacade(controller);
    }


    @Bean
    @ConditionalOnMissingBean(ProcessorLoadBalancingControllerFacade.class)
    public ProcessorLoadBalancingControllerFacade processorLoadBalancingControllerFacade(
            AdminProcessorLoadBalancingService processorLoadBalancingController,
            RaftConfigServiceFactory raftServiceFactory) {
        return new RaftProcessorLoadBalancingControllerFacade(processorLoadBalancingController, raftServiceFactory);
    }

    @Bean
    public StandaloneTaskManager localTaskManager(ScheduledTaskExecutor taskExecutor,
                                                  TaskRepository taskRepository,
                                                  TaskPayloadSerializer taskPayloadSerializer,
                                                  PlatformTransactionManager platformTransactionManager,
                                                  @Qualifier("taskScheduler") ScheduledExecutorService scheduler,
                                                  Clock clock) {
        return new StandaloneTaskManager("_local",
                                         taskExecutor,
                                         taskRepository,
                                         taskPayloadSerializer,
                                         platformTransactionManager,
                                         scheduler,
                                         clock);
    }
}
