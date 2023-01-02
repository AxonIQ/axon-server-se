/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
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
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.LocalEventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.LocalTransformers;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStoreSupplier;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.AutoCloseableEventProvider;
import io.axoniq.axonserver.localstorage.ContextEventIteratorFactory;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.transformation.EventStoreTransformer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Paths;

@Configuration
public class TransformationConfiguration {

    @Bean
    public ContextEventIteratorFactory eventProviderFactory(LocalEventStore eventStore) {
        return context -> {
            AutoCloseableEventProvider autoCloseableEventProvider =
                    new AutoCloseableEventProvider(token -> eventStore.eventIterator(context, token));
            return new EventProvider() {
                @Override
                public Mono<Event> event(long token) {
                    return autoCloseableEventProvider.event(token);
                }

                @Override
                public Mono<Void> close() {
                    return autoCloseableEventProvider.close();
                }
            };
        };
    }

    @Bean
    public TransformationEntryStoreSupplier transformationEntryStoreSupplier(EmbeddedDBProperties embeddedDBProperties) {
        DefaultTransformationEntryStoreSupplier.StoragePropertiesSupplier storagePropertiesSupplier =
                (context, transformationId) -> {
            String baseDirectory = embeddedDBProperties.getEvent().getStorage(context);
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
                                     ContextEventIteratorFactory iteratorFactory,
                                     TransformationEntryStoreSupplier transformationEntryStoreSupplier,
                                     EventStoreStateStore eventStoreStateStore) {
        return new LocalTransformers(iteratorFactory::createFrom,
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
            TransformationEntryStoreSupplier transformationEntryStoreSupplier,
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
            TransformationEntryStoreSupplier transformationEntryStoreSupplier,
            EventStoreTransformationRepository repository,
            CleanedTransformationRepository cleanedTransformationRepository) {
        return new DefaultTransformationCleanTask(new DefaultTransformationCleanExecutor(
                transformationEntryStoreSupplier),
                                                  new JpaTransformationsToBeCleaned(repository,
                                                                                    cleanedTransformationRepository));
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public EventStoreTransformationService localEventStoreTransformationService(Transformers transformers,
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
}
