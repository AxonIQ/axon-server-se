/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.AxonServerStandardAccessController;
import io.axoniq.axonserver.access.roles.RoleController;
import io.axoniq.axonserver.admin.user.api.UserAdminService;
import io.axoniq.axonserver.admin.user.requestprocessor.LocalUserAdminService;
import io.axoniq.axonserver.admin.user.requestprocessor.UserController;
import io.axoniq.axonserver.exception.CriticalEventException;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.DefaultInstructionAckSource;
import io.axoniq.axonserver.grpc.InstructionAckSource;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.event.EventSchedulerGrpc;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.localstorage.DefaultEventDecorator;
import io.axoniq.axonserver.localstorage.EventDecorator;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.StandardEventStoreFactory;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.message.event.EventSchedulerService;
import io.axoniq.axonserver.message.query.QueryHandlerSelector;
import io.axoniq.axonserver.message.query.RoundRobinQueryHandlerSelector;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.plugin.AxonServerInformationProvider;
import io.axoniq.axonserver.taskscheduler.ScheduledTaskExecutor;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.TaskPayloadSerializer;
import io.axoniq.axonserver.taskscheduler.TaskRepository;
import io.axoniq.axonserver.topology.DefaultEventStoreLocator;
import io.axoniq.axonserver.topology.DefaultTopology;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.DaemonThreadFactory;
import io.axoniq.axonserver.version.DefaultVersionInfoProvider;
import io.axoniq.axonserver.version.VersionInfoProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationNotAllowedException;
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
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nonnull;

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
    @ConditionalOnMissingBean(AxonServerAccessController.class)
    public AxonServerAccessController axonServerAccessController(MessagingPlatformConfiguration messagingPlatformConfiguration,
                                                                 UserController userController) {
        return new AxonServerStandardAccessController(messagingPlatformConfiguration, userController);
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
        return newScheduledThreadPool(10, new DaemonThreadFactory("task-scheduler"));
    }

    @Bean
    @ConditionalOnMissingBean(UserAdminService.class)
    public UserAdminService userAdminService(UserController userController,
                                             ApplicationEventPublisher eventPublisher,
                                             RoleController roleController) {
        return getUserAdminService(userController, eventPublisher, roleController);
    }

    @Nonnull
    private UserAdminService getUserAdminService(UserController userController,
                                                 ApplicationEventPublisher eventPublisher,
                                                 RoleController roleController) {
        return new LocalUserAdminService(userController, eventPublisher, roleController);
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
            protected void invokeListener(@Nonnull ApplicationListener<?> listener, @Nonnull ApplicationEvent event) {
                try {
                    super.invokeListener(listener, event);
                } catch (BeanCreationNotAllowedException ex) {
                    // shutdown in progress
                } catch (CriticalEventException ex) {
                    throw ex;
                } catch (RuntimeException ex) {
                    logger.warn("Invoking listener {} failed", listener, ex);
                }
            }
        };
    }

    @Bean
    @ConditionalOnMissingBean(AxonServerInformationProvider.class)
    public AxonServerInformationProvider axonServerInformationProvider(VersionInfoProvider versionInfoProvider,
                                                                       FeatureChecker featureChecker) {
        Map<String, String> versionInfo = new HashMap<>();
        versionInfo.put(AxonServerInformationProvider.EDITION, featureChecker.getEdition());
        versionInfo.put(AxonServerInformationProvider.VERSION, versionInfoProvider.get().getVersion());
        return () -> versionInfo;
    }

    @Bean
    public DiskSpaceHealthIndicator diskSpaceHealthIndicator() {
        //disable regular diskSpaceHealthIndicator bean
        //using FileSystemMonitor instead
        return null;
    }

}
