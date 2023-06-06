/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.eventstore.transformation.ActionScheduledTask;
import io.axoniq.axonserver.eventstore.transformation.MultiScheduledTask;
import io.axoniq.axonserver.eventstore.transformation.TransformationTask;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.apply.CleanTransformationApplied;
import io.axoniq.axonserver.eventstore.transformation.apply.CleanTransformationProgressStore;
import io.axoniq.axonserver.eventstore.transformation.apply.DefaultTransformationApplyExecutor;
import io.axoniq.axonserver.eventstore.transformation.apply.LocalMarkTransformationApplied;
import io.axoniq.axonserver.eventstore.transformation.apply.MarkTransformationApplied;
import io.axoniq.axonserver.eventstore.transformation.apply.TransformationApplyAction;
import io.axoniq.axonserver.eventstore.transformation.apply.TransformationApplyExecutor;
import io.axoniq.axonserver.eventstore.transformation.apply.TransformationProgressStore;
import io.axoniq.axonserver.eventstore.transformation.clean.DefaultTransformationCleanExecutor;
import io.axoniq.axonserver.eventstore.transformation.clean.TransformationCleanAction;
import io.axoniq.axonserver.eventstore.transformation.clean.TransformationsToBeCleaned;
import io.axoniq.axonserver.eventstore.transformation.clean.transformations.AppliedTransformations;
import io.axoniq.axonserver.eventstore.transformation.clean.transformations.CancelledTransformations;
import io.axoniq.axonserver.eventstore.transformation.clean.transformations.FileSystemTransformations;
import io.axoniq.axonserver.eventstore.transformation.clean.transformations.MissingTransformations;
import io.axoniq.axonserver.eventstore.transformation.clean.transformations.MultipleTransformations;
import io.axoniq.axonserver.eventstore.transformation.compact.CompactingContexts;
import io.axoniq.axonserver.eventstore.transformation.compact.DefaultEventStoreCompactionExecutor;
import io.axoniq.axonserver.eventstore.transformation.compact.EventStoreCompactAction;
import io.axoniq.axonserver.eventstore.transformation.compact.EventStoreCompactionExecutor;
import io.axoniq.axonserver.eventstore.transformation.compact.LocalMarkEventStoreCompacted;
import io.axoniq.axonserver.eventstore.transformation.compact.MarkEventStoreCompacted;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreStateRepository;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationRepository;
import io.axoniq.axonserver.eventstore.transformation.jpa.JpaCompactingContexts;
import io.axoniq.axonserver.eventstore.transformation.jpa.JpaEventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.jpa.JpaLocalTransformationProgressStore;
import io.axoniq.axonserver.eventstore.transformation.jpa.JpaTransformations;
import io.axoniq.axonserver.eventstore.transformation.jpa.JpaTransformationsInProgressForContext;
import io.axoniq.axonserver.eventstore.transformation.jpa.LocalEventStoreTransformationRepository;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultTransformationEntryStoreSupplier;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventProvider;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.FastValidationEventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.LocalEventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.LocalTransformers;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.LoggingEventTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationBaseStorageProvider;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStoreProvider;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import io.axoniq.axonserver.eventstore.transformation.spi.TransformationAllowed;
import io.axoniq.axonserver.eventstore.transformation.spi.TransformationsInProgressForContext;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import io.axoniq.axonserver.localstorage.AutoCloseableEventProvider;
import io.axoniq.axonserver.localstorage.ContextEventProviderSupplier;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.transformation.EventStoreTransformer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.PlatformTransactionManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
@ConditionalOnProperty(value = "axoniq.axonserver.preview.event-transformation")
public class TransformationConfiguration {

    // TODO: 1/30/23 extract this class
    private static class CachedContextEventProviderSupplier implements ContextEventProviderSupplier {

        private final ContextEventProviderSupplier supplier;
        private final Map<String, EventProvider> cache = new ConcurrentHashMap<>();

        private CachedContextEventProviderSupplier(ContextEventProviderSupplier supplier) {
            this.supplier = supplier;
        }

        @Override
        public EventProvider eventProviderFor(String context) {
            return cache.computeIfAbsent(context, supplier::eventProviderFor);
        }
    }

    @Bean
    public ContextEventProviderSupplier localEventProviderSupplier(LocalEventStore eventStore) {
        ContextEventProviderSupplier autoCloseable = ctx -> {
            if (eventStore.activeContext(ctx)) {
                return new AutoCloseableEventProvider(token -> eventStore.eventIterator(ctx, token))::event;
            }
            return (EventProvider) token -> Mono.empty();
        };
        CachedContextEventProviderSupplier cached = new CachedContextEventProviderSupplier(autoCloseable);
        return cached::eventProviderFor;
    }


    @Bean
    public TransformationEntryStoreProvider transformationEntryStoreSupplier(
            TransformationBaseStorageProvider baseStorageProvider) {
        DefaultTransformationEntryStoreSupplier.StoragePropertiesSupplier storagePropertiesSupplier =
                (context, transformationId) -> {
                    Path path = Paths.get(baseStorageProvider.storageLocation(), context, transformationId);
                    StorageProperties storageProperties = new StorageProperties();
                    storageProperties.setStorage(path.toFile());
                    storageProperties.setSuffix(".actions");
                    return storageProperties;
                };
        return new DefaultTransformationEntryStoreSupplier(storagePropertiesSupplier);
    }

    @Bean
    public EventStoreStateStore eventStoreStateStore(EventStoreStateRepository repository) {
        return new JpaEventStoreStateStore(repository);
    }

    @Bean
    public Transformers transformers(EventStoreTransformationRepository repository,
                                     ContextEventProviderSupplier iteratorFactory,
                                     TransformationEntryStoreProvider transformationEntryStoreSupplier,
                                     EventStoreStateStore eventStoreStateStore,
                                     PlatformTransactionManager transactionManager) {
        return new LocalTransformers(repository,
                                     transformationEntryStoreSupplier,
                                     eventStoreStateStore,
                                     transactionManager);
    }

    @Bean
    public Transformations transformations(EventStoreTransformationRepository repository) {
        return new JpaTransformations(repository);
    }

    @Bean
    public TransformationProgressStore localTransformationProgressStore(
            LocalEventStoreTransformationRepository repository) {
        return new JpaLocalTransformationProgressStore(repository);
    }


    @Bean
    public TransformationApplyExecutor localTransformationApplier(
            TransformationEntryStoreProvider transformationEntryStoreSupplier,
            TransformationProgressStore localTransformationProgressStore,
            EventStoreTransformer eventStoreTransformer) {
        return new DefaultTransformationApplyExecutor(transformationEntryStoreSupplier,
                                                      localTransformationProgressStore,
                                                      eventStoreTransformer::transformEvents);
    }


    @Bean
    public MarkTransformationApplied localMarkTransformationApplied(Transformers transformers) {
        return new LocalMarkTransformationApplied(transformers);
    }

    @Bean
    public MarkEventStoreCompacted localMarkEventStoreCompacted(Transformers transformers) {
        return new LocalMarkEventStoreCompacted(transformers);
    }

    @Bean
    public CleanTransformationApplied cleanTransformationProgressStore(TransformationProgressStore store) {
        return new CleanTransformationProgressStore(store);
    }

    @Bean
    @Primary
    public CleanTransformationApplied cleanTransformationApplied(Collection<CleanTransformationApplied> collection) {
        return () -> Flux.fromIterable(collection)
                         .flatMap(CleanTransformationApplied::clean)
                         .then();
    }

    @Bean
    public TransformationTask transformationApplyTask(TransformationApplyExecutor applier,
                                                      MarkTransformationApplied markTransformationApplied,
                                                      CleanTransformationApplied cleanTransformationApplied,
                                                      Transformations transformations) {
        return new ActionScheduledTask(
                new TransformationApplyAction(applier, markTransformationApplied,
                                              cleanTransformationApplied,
                                              transformations));
    }

    @Bean
    public EventStoreCompactionExecutor transformationCompactionExecutor(
            EventStoreTransformer eventStoreTransformer) {
        return new DefaultEventStoreCompactionExecutor(eventStoreTransformer::compact);
    }

    @Bean
    public CompactingContexts compactingContexts(EventStoreStateRepository repository) {
        return new JpaCompactingContexts(repository);
    }

    @Bean
    public TransformationTask transformationCompactionTask(
            EventStoreCompactionExecutor eventStoreCompactionExecutor,
            CompactingContexts compactingContexts,
            MarkEventStoreCompacted markEventStoreCompacted) {
        return new ActionScheduledTask(
                new EventStoreCompactAction(Flux.fromIterable(compactingContexts),
                                            eventStoreCompactionExecutor,
                                            markEventStoreCompacted));
    }

    @Bean
    public TransformationBaseStorageProvider transformationBaseStorageProvider(
            @Value("${axoniq.axonserver.transformation.storage:transformation}") String transformationBasePath
    ) {
        return () -> transformationBasePath;
    }


    @Bean
    public TransformationsToBeCleaned appliedTransformationsToBeCleaned(
            TransformationBaseStorageProvider transformationBaseStorageProvider,
            EventStoreTransformationRepository eventStoreTransformationRepository
    ) {
        return new AppliedTransformations(new FileSystemTransformations(transformationBaseStorageProvider),
                                          eventStoreTransformationRepository);
    }

    @Bean
    public TransformationsToBeCleaned cancelledTransformationsToBeCleaned(
            TransformationBaseStorageProvider transformationBaseStorageProvider,
            EventStoreTransformationRepository eventStoreTransformationRepository
    ) {
        return new CancelledTransformations(new FileSystemTransformations(transformationBaseStorageProvider),
                                            eventStoreTransformationRepository);
    }


    @Bean
    public TransformationsToBeCleaned missingTransformationsToBeCleaned(
            TransformationBaseStorageProvider transformationBaseStorageProvider,
            EventStoreTransformationRepository eventStoreTransformationRepository
    ) {
        return new MissingTransformations(new FileSystemTransformations(transformationBaseStorageProvider),
                                          eventStoreTransformationRepository);
    }

    @Primary
    @Bean
    public TransformationsToBeCleaned transformationsToBeCleaned(
            Collection<TransformationsToBeCleaned> transformationsToBeCleanedCollection
    ) {
        return new MultipleTransformations(transformationsToBeCleanedCollection);
    }

    @Bean
    public TransformationTask transformationCleanTask(
            TransformationsToBeCleaned transformationsToBeCleaned,
            TransformationEntryStoreProvider transformationEntryStoreSupplier) {
        return new ActionScheduledTask(new TransformationCleanAction(new DefaultTransformationCleanExecutor(
                transformationEntryStoreSupplier), transformationsToBeCleaned));
    }

    @Primary
    @Bean
    public TransformationTask multiScheduledTask(Collection<TransformationTask> scheduledTasks) {
        return new MultiScheduledTask(scheduledTasks);
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public LocalEventStoreTransformationService localEventStoreTransformationService(Transformers transformers,
                                                                                     Transformations transformations,
                                                                                     TransformationTask scheduledTask) {
        return new LocalEventStoreTransformationService(transformers,
                                                        transformations,
                                                        scheduledTask);
    }

    @Bean(destroyMethod = "destroy")
    public FastValidationEventStoreTransformationService fastValidationEventStoreTransformationService(
            @Qualifier("localEventStoreTransformationService") EventStoreTransformationService service,
            ContextEventProviderSupplier eventIteratorFactory,
            TransformationAllowed transformationAllowed
    ) {
        return new FastValidationEventStoreTransformationService(service, eventIteratorFactory::eventProviderFor,
                                                                 transformationAllowed);
    }

    @Primary
    @ConditionalOnMissingBean(value = EventStoreTransformationService.class, ignored = {
            LocalEventStoreTransformationService.class,
            FastValidationEventStoreTransformationService.class
    })
    @Bean
    public EventStoreTransformationService loggingEventStoreTransformationService(
            @Qualifier("fastValidationEventStoreTransformationService") EventStoreTransformationService service
    ) {
        return new LoggingEventTransformationService(service);
    }


    @Bean
    public TransformationsInProgressForContext transformationsInProgressForContext(
            EventStoreStateRepository repository) {
        return new JpaTransformationsInProgressForContext(repository);
    }

    @Primary
    @Bean
    public TransformationAllowed transformationAllowed(List<TransformationAllowed> transformationAllowedList) {
        return context -> Flux.fromIterable(transformationAllowedList)
                .flatMap(transformationAllowed -> transformationAllowed.validate(context))
                .then();
    }
}
