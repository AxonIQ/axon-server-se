/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.apply.DefaultTransformationApplyExecutor;
import io.axoniq.axonserver.eventstore.transformation.apply.DefaultTransformationApplyTask;
import io.axoniq.axonserver.eventstore.transformation.apply.LocalMarkTransformationApplied;
import io.axoniq.axonserver.eventstore.transformation.apply.MarkTransformationApplied;
import io.axoniq.axonserver.eventstore.transformation.apply.TransformationApplyExecutor;
import io.axoniq.axonserver.eventstore.transformation.apply.TransformationApplyTask;
import io.axoniq.axonserver.eventstore.transformation.apply.TransformationProgressStore;
import io.axoniq.axonserver.eventstore.transformation.clean.CleanedTransformationRepository;
import io.axoniq.axonserver.eventstore.transformation.clean.DefaultTransformationCleanExecutor;
import io.axoniq.axonserver.eventstore.transformation.clean.DefaultTransformationCleanTask;
import io.axoniq.axonserver.eventstore.transformation.clean.JpaTransformationsToBeCleaned;
import io.axoniq.axonserver.eventstore.transformation.clean.TransformationCleanTask;
import io.axoniq.axonserver.eventstore.transformation.compact.CompactingContexts;
import io.axoniq.axonserver.eventstore.transformation.compact.DefaultEventStoreCompactionExecutor;
import io.axoniq.axonserver.eventstore.transformation.compact.DefaultEventStoreCompactionTask;
import io.axoniq.axonserver.eventstore.transformation.compact.EventStoreCompactionExecutor;
import io.axoniq.axonserver.eventstore.transformation.compact.EventStoreCompactionTask;
import io.axoniq.axonserver.eventstore.transformation.compact.LocalMarkEventStoreCompacted;
import io.axoniq.axonserver.eventstore.transformation.compact.MarkEventStoreCompacted;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreStateRepository;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationRepository;
import io.axoniq.axonserver.eventstore.transformation.jpa.JpaCompactingContexts;
import io.axoniq.axonserver.eventstore.transformation.jpa.JpaEventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.jpa.JpaLocalTransformationProgressStore;
import io.axoniq.axonserver.eventstore.transformation.jpa.JpaTransformations;
import io.axoniq.axonserver.eventstore.transformation.jpa.LocalEventStoreTransformationRepository;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultTransformationEntryStoreSupplier;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventProvider;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.FastValidationEventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.LocalEventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.LocalTransformers;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStoreProvider;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import io.axoniq.axonserver.localstorage.AutoCloseableEventProvider;
import io.axoniq.axonserver.localstorage.ContextEventProviderSupplier;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.transformation.EventStoreTransformer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
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
        return context -> new CachedContextEventProviderSupplier(autoCloseable).eventProviderFor(context);
    }

    @Bean
    public TransformationEntryStoreProvider transformationEntryStoreSupplier(
            EmbeddedDBProperties embeddedDBProperties) {
        DefaultTransformationEntryStoreSupplier.StoragePropertiesSupplier storagePropertiesSupplier =
                (context, transformationId) -> {
                    String baseDirectory = embeddedDBProperties.getEvent().getPrimaryStorage(context);
                    StorageProperties storageProperties = new StorageProperties();
                    storageProperties.setStorage(Paths.get(baseDirectory, "transformation", transformationId).toFile());
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
                                     EventStoreStateStore eventStoreStateStore) {
        return new LocalTransformers(iteratorFactory::eventProviderFor,
                                     repository,
                                     transformationEntryStoreSupplier,
                                     eventStoreStateStore);
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
    public TransformationApplyTask transformationApplyTask(TransformationApplyExecutor applier,
                                                           MarkTransformationApplied markTransformationApplied,
                                                           Transformations transformations) {
        return new DefaultTransformationApplyTask(applier, markTransformationApplied, transformations);
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
    public EventStoreCompactionTask transformationCompactionTask(
            EventStoreCompactionExecutor eventStoreCompactionExecutor,
            CompactingContexts compactingContexts,
            MarkEventStoreCompacted markEventStoreCompacted) {
        return new DefaultEventStoreCompactionTask(Flux.fromIterable(compactingContexts),
                                                   eventStoreCompactionExecutor,
                                                   markEventStoreCompacted);
    }


    @Bean
    public TransformationCleanTask transformationCleanTask(
            TransformationEntryStoreProvider transformationEntryStoreSupplier,
            EventStoreTransformationRepository repository,
            CleanedTransformationRepository cleanedTransformationRepository) {
        return new DefaultTransformationCleanTask(new DefaultTransformationCleanExecutor(
                transformationEntryStoreSupplier),
                                                  new JpaTransformationsToBeCleaned(repository,
                                                                                    cleanedTransformationRepository));
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public LocalEventStoreTransformationService localEventStoreTransformationService(Transformers transformers,
                                                                                Transformations transformations,
                                                                                EventStoreCompactionTask eventStoreCompactionTask,
                                                                                TransformationApplyTask transformationApplyTask,
                                                                                TransformationCleanTask transformationCleanTask) {
        return new LocalEventStoreTransformationService(transformers,
                                                        transformations,
                                                        eventStoreCompactionTask,
                                                        transformationApplyTask,
                                                        transformationCleanTask);
    }

    @ConditionalOnMissingBean(value = EventStoreTransformationService.class,
            ignored = LocalEventStoreTransformationService.class)
    @Primary
    @Bean(destroyMethod = "destroy")
    public FastValidationEventStoreTransformationService fastValidationEventStoreTransformationService(
            @Qualifier("localEventStoreTransformationService") EventStoreTransformationService service,
            ContextEventProviderSupplier eventIteratorFactory
    ) {
        return new FastValidationEventStoreTransformationService(service, eventIteratorFactory::eventProviderFor);
    }
}
