package io.axoniq.axonserver.config;

import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.applicationevents.UserEvents;
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
import io.axoniq.axonserver.rest.UserControllerFacade;
import io.axoniq.axonserver.topology.DefaultEventStoreLocator;
import io.axoniq.axonserver.topology.DefaultTopology;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.List;

/**
 * Creates instances of Spring beans required by Axon Server.
 *
 * @author Marc Gathier
 */
@Configuration
public class AxonServerStandardConfiguration {

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
        return new FeatureChecker() {};
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    @ConditionalOnMissingBean(UserControllerFacade.class)
    public UserControllerFacade userControllerFacade(UserController userController, ApplicationEventPublisher eventPublisher) {
        return new UserControllerFacade() {
            @Override
            public void updateUser(String userName, String password, String[] roles) {
                User updatedUser = userController.updateUser(userName, password, roles);
                eventPublisher.publishEvent(new UserEvents.UserUpdated(updatedUser, false));

            }

            @Override
            public List<User> getUsers() {
                return userController.getUsers();
            }

            @Override
            public void deleteUser(String name) {
                userController.deleteUser(name);
                eventPublisher.publishEvent(new UserEvents.UserDeleted(name, false));
            }
        };
    }

}
