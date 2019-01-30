package io.axoniq.axonserver.config;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalanceStrategyController;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancingController;
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
import io.axoniq.axonserver.message.query.RoundRobinQueryHandlerSelector;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.rest.LoadBalanceStrategyControllerFacade;
import io.axoniq.axonserver.rest.ProcessorLoadBalancingControllerFacade;
import io.axoniq.axonserver.rest.UserControllerFacade;
import io.axoniq.axonserver.serializer.Printable;
import io.axoniq.axonserver.topology.DefaultEventStoreLocator;
import io.axoniq.axonserver.topology.DefaultTopology;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.platform.user.User;
import io.axoniq.platform.user.UserController;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.List;

/**
 * @author Marc Gathier
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
    public EventStoreLocator eventStoreLocator(LocalEventStore localEventStore, MessagingPlatformConfiguration messagingPlatformConfiguration) {
        return new DefaultEventStoreLocator(localEventStore, messagingPlatformConfiguration);
    }

    @Bean
    @ConditionalOnMissingBean(FeatureChecker.class)
    public FeatureChecker featureChecker() {
        return new DefaultFeatureChecker();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    @ConditionalOnMissingBean(UserControllerFacade.class)
    public UserControllerFacade userControllerFacade(UserController userController) {
        return new UserControllerFacade() {
            @Override
            public void updateUser(String userName, String password, String[] roles) {
                userController.updateUser(userName, password, roles);
            }

            @Override
            public List<User> getUsers() {
                return userController.getUsers();
            }

            @Override
            public void deleteUser(String name) {
                userController.deleteUser(name);
            }
        };
    }

    @Bean
    @ConditionalOnMissingBean(LoadBalanceStrategyControllerFacade.class)
    public LoadBalanceStrategyControllerFacade loadBalanceStrategyControllerFacade(LoadBalanceStrategyController controller) {
        return new LoadBalanceStrategyControllerFacade() {
            @Override
            public Iterable<? extends Printable> findAll() {
                return controller.findAll();
            }

            @Override
            public void save(LoadBalancingStrategy loadBalancingStrategy) {
                controller.save(loadBalancingStrategy);
            }

            @Override
            public void delete(String strategyName) {
                controller.delete(strategyName);
            }

            @Override
            public void updateFactoryBean(String strategyName, String factoryBean) {
                controller.updateFactoryBean(strategyName, factoryBean);
            }

            @Override
            public void updateLabel(String strategyName, String label) {
                controller.updateLabel(strategyName, label);

            }

            @Override
            public LoadBalancingStrategy findByName(String strategyName) {
                return controller.findByName(strategyName);
            }
        };
    }


    @Bean
    @ConditionalOnMissingBean(ProcessorLoadBalancingControllerFacade.class)
    public ProcessorLoadBalancingControllerFacade processorLoadBalancingControllerFacade(
            ProcessorLoadBalancingController processorLoadBalancingController) {
        return new ProcessorLoadBalancingControllerFacade() {
            @Override
            public void save(ProcessorLoadBalancing processorLoadBalancing) {
                processorLoadBalancingController.save(processorLoadBalancing);
            }

            @Override
            public List<ProcessorLoadBalancing> findByComponentAndContext(String component, String context) {
                return processorLoadBalancingController.findByComponentAndContext(component, context);
            }
        };
    }



}
