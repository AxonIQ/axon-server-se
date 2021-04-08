/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.event.EventSchedulerGrpc;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.refactoring.configuration.topology.DefaultTopology;
import io.axoniq.axonserver.refactoring.configuration.topology.Topology;
import io.axoniq.axonserver.refactoring.messaging.MessagingPlatformException;
import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommandProviderInbound;
import io.axoniq.axonserver.refactoring.messaging.query.QueryHandlerSelector;
import io.axoniq.axonserver.refactoring.messaging.query.RoundRobinQueryHandlerSelector;
import io.axoniq.axonserver.refactoring.metric.DefaultMetricCollector;
import io.axoniq.axonserver.refactoring.metric.MeterFactory;
import io.axoniq.axonserver.refactoring.metric.MetricCollector;
import io.axoniq.axonserver.refactoring.security.access.UserEvents;
import io.axoniq.axonserver.refactoring.security.access.jpa.User;
import io.axoniq.axonserver.refactoring.security.access.jpa.UserRole;
import io.axoniq.axonserver.refactoring.security.access.user.UserController;
import io.axoniq.axonserver.refactoring.security.access.user.UserControllerFacade;
import io.axoniq.axonserver.refactoring.store.DefaultEventDecorator;
import io.axoniq.axonserver.refactoring.store.DefaultEventStoreLocator;
import io.axoniq.axonserver.refactoring.store.EventDecorator;
import io.axoniq.axonserver.refactoring.store.EventStoreFactory;
import io.axoniq.axonserver.refactoring.store.EventStoreLocator;
import io.axoniq.axonserver.refactoring.store.LocalEventStore;
import io.axoniq.axonserver.refactoring.store.engine.file.EmbeddedDBProperties;
import io.axoniq.axonserver.refactoring.store.engine.file.StandardEventStoreFactory;
import io.axoniq.axonserver.refactoring.store.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.refactoring.store.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.refactoring.store.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.refactoring.store.transformation.EventTransformerFactory;
import io.axoniq.axonserver.refactoring.taskscheduler.ScheduledTaskExecutor;
import io.axoniq.axonserver.refactoring.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.refactoring.taskscheduler.TaskPayloadSerializer;
import io.axoniq.axonserver.refactoring.taskscheduler.TaskRepository;
import io.axoniq.axonserver.refactoring.transport.grpc.AxonServerClientService;
import io.axoniq.axonserver.refactoring.transport.grpc.EventSchedulerService;
import io.axoniq.axonserver.refactoring.transport.instruction.DefaultInstructionAckSource;
import io.axoniq.axonserver.refactoring.transport.instruction.InstructionAckSource;
import io.axoniq.axonserver.refactoring.transport.rest.ExternalLoginsProvider;
import io.axoniq.axonserver.refactoring.transport.rest.actuator.FileSystemMonitor;
import io.axoniq.axonserver.refactoring.version.DefaultVersionInfoProvider;
import io.axoniq.axonserver.refactoring.version.VersionInfoProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.system.DiskSpaceHealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.newScheduledThreadPool;

/**
 * Creates instances of Spring beans required by Axon Server.
 *
 * @author Marc Gathier
 */
@Configuration
public class AxonServerStandardConfiguration {

    private final Logger logger = LoggerFactory.getLogger(AxonServerStandardConfiguration.class);

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
    public EventStoreFactory eventStoreFactory(EmbeddedDBProperties embeddedDBProperties,
                                               EventTransformerFactory eventTransformerFactory,
                                               StorageTransactionManagerFactory storageTransactionManagerFactory,
                                               MeterFactory meterFactory,
                                               FileSystemMonitor fileSystemMonitor) {
        return new StandardEventStoreFactory(embeddedDBProperties,
                                             eventTransformerFactory,
                                             storageTransactionManagerFactory,
                                             meterFactory, fileSystemMonitor);
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
        return new FeatureChecker() {
        };
    }

    @Bean
    @ConditionalOnMissingBean(EventSchedulerGrpc.EventSchedulerImplBase.class)
    public AxonServerClientService eventSchedulerService(StandaloneTaskManager localTaskManager,
                                                         TaskPayloadSerializer taskPayloadSerializer) {
        logger.info("Creating SE EventSchedulerService");
        return new EventSchedulerService(localTaskManager, taskPayloadSerializer);
    }

    @Bean
    @ConditionalOnMissingBean(EventDecorator.class)
    public EventDecorator eventDecorator() {
        return new DefaultEventDecorator();
    }

    @Bean
    @ConditionalOnMissingBean(ExternalLoginsProvider.class)
    public ExternalLoginsProvider externalLoginProvider() {
        return Collections::emptyList;
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }


    @Bean
    @Qualifier("taskScheduler")
    public ScheduledExecutorService scheduler() {
        return newScheduledThreadPool(10, new CustomizableThreadFactory("task-scheduler"));
    }

    @Bean
    @ConditionalOnMissingBean(UserControllerFacade.class)
    public UserControllerFacade userControllerFacade(UserController userController,
                                                     ApplicationEventPublisher eventPublisher) {
        return new UserControllerFacade() {
            @Override
            public void updateUser(String userName, String password, Set<UserRole> roles) {
                validateContexts(roles);
                User updatedUser = userController.updateUser(userName, password, roles);
                eventPublisher.publishEvent(new UserEvents.UserUpdated(updatedUser, false));

            }

            private void validateContexts(Set<UserRole> roles) {
                if (roles == null) {
                    return;
                }
                if (roles.stream().anyMatch(userRole -> !validContext(userRole.getContext()))) {
                    throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                         "Only specify context default for standard edition");
                }
            }

            private boolean validContext(String context) {
                return context == null || context.equals(Topology.DEFAULT_CONTEXT) || context.equals("*");
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

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    @Qualifier("platformInstructionAckSource")
    public InstructionAckSource<PlatformOutboundInstruction> platformInstructionAckSource() {
        return new DefaultInstructionAckSource<>(ack -> PlatformOutboundInstruction.newBuilder()
                                                                                   .setAck(ack)
                                                                                   .build());
    }

    @Bean
    @Qualifier("commandInstructionAckSource")
    public InstructionAckSource<SerializedCommandProviderInbound> commandInstructionAckSource() {
        return new DefaultInstructionAckSource<>(ack -> new SerializedCommandProviderInbound(CommandProviderInbound
                                                                                                     .newBuilder()
                                                                                                     .setAck(ack)
                                                                                                     .build()));
    }

    @Bean
    @Qualifier("queryInstructionAckSource")
    public InstructionAckSource<QueryProviderInbound> queryInstructionAckSource() {
        return new DefaultInstructionAckSource<>(ack -> QueryProviderInbound.newBuilder()
                                                                            .setAck(ack)
                                                                            .build());
    }

    /**
     * Creates a default version information provider bean.
     *
     * @return a default version information provider
     */
    @Bean
    @ConditionalOnMissingBean(VersionInfoProvider.class)
    public VersionInfoProvider versionInfoProvider() {
        return new DefaultVersionInfoProvider();
    }

    @Bean
    @ConditionalOnMissingBean(StandaloneTaskManager.class)
    public StandaloneTaskManager localTaskManager(ScheduledTaskExecutor taskExecutor,
                                                  TaskRepository taskRepository,
                                                  TaskPayloadSerializer taskPayloadSerializer,
                                                  PlatformTransactionManager platformTransactionManager,
                                                  @Qualifier("taskScheduler") ScheduledExecutorService scheduler,
                                                  Clock clock) {
        return new StandaloneTaskManager(Topology.DEFAULT_CONTEXT,
                                         taskExecutor,
                                         taskRepository,
                                         taskPayloadSerializer,
                                         platformTransactionManager,
                                         scheduler,
                                         clock);
    }


    @Bean
    public ApplicationEventMulticaster applicationEventMulticaster() {
        return new SimpleApplicationEventMulticaster() {
            @Override
            protected void invokeListener(ApplicationListener<?> listener, ApplicationEvent event) {
                try {
                    super.invokeListener(listener, event);
                } catch (RuntimeException ex) {
                    logger.warn("Invoking listener {} failed", listener, ex);
                }
            }
        };
    }

    @Bean
    public DiskSpaceHealthIndicator diskSpaceHealthIndicator() {
        //disable regular diskSpaceHealthIndicator bean
        //using FileSystemMonitor instead
        return null;
    }

}
