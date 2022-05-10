package io.axoniq.axonserver.config;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.ApplyCompletedConsumer;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultEventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultTransformationApplyTask;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultTransformationCancelTask;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultTransformationEntryStoreSupplier;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultTransformationRollBackTask;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationRepository;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.JpaTransformations;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.LocalTransformers;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.RollbackCompletedConsumer;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationApplyExecutor;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationApplyTask;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationCancelExecutor;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationCancelTask;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStoreSupplier;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationRollBackTask;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationRollbackExecutor;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import io.axoniq.axonserver.localstorage.AutoCloseableEventProvider;
import io.axoniq.axonserver.localstorage.ContextEventIteratorFactory;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import io.axoniq.axonserver.localstorage.transformation.DefaultLocalTransformationApplyExecutor;
import io.axoniq.axonserver.localstorage.transformation.DefaultLocalTransformationCancelExecutor;
import io.axoniq.axonserver.localstorage.transformation.DefaultLocalTransformationRollbackExecutor;
import io.axoniq.axonserver.localstorage.transformation.EventStoreTransformationProgressRepository;
import io.axoniq.axonserver.localstorage.transformation.JpaLocalTransformationProgressStore;
import io.axoniq.axonserver.localstorage.transformation.LocalEventStoreTransformer;
import io.axoniq.axonserver.localstorage.transformation.LocalTransformationApplyExecutor;
import io.axoniq.axonserver.localstorage.transformation.LocalTransformationCancelExecutor;
import io.axoniq.axonserver.localstorage.transformation.LocalTransformationProgressStore;
import io.axoniq.axonserver.localstorage.transformation.LocalTransformationRollbackExecutor;
import io.axoniq.axonserver.localstorage.transformation.StandardTransformationApplyExecutor;
import io.axoniq.axonserver.localstorage.transformation.StandardTransformationCancelExecutor;
import io.axoniq.axonserver.localstorage.transformation.StandardTransformationRollbackExecutor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Paths;

@Configuration
public class TransformationConfiguration {

    @Bean
    public ContextEventIteratorFactory eventProviderFactory(LocalEventStore eventStore) {
        return context -> {
            AutoCloseableEventProvider autoCloseableEventProvider =
                    new AutoCloseableEventProvider(token -> eventStore.eventIterator(context, token));
            return autoCloseableEventProvider::event;
        };
    }

    @Bean
    public TransformationEntryStoreSupplier transformationEntryStoreSupplier(EmbeddedDBProperties embeddedDBProperties) {
        DefaultTransformationEntryStoreSupplier.StoragePropertiesSupplier storagePropertiesSupplier = context -> {
            String baseDirectory = embeddedDBProperties.getEvent().getStorage(context);
            StorageProperties storageProperties = new StorageProperties();
            storageProperties.setStorage(Paths.get(baseDirectory).toFile());
            storageProperties.setSuffix(".actions");
            return storageProperties;
        };
        return new DefaultTransformationEntryStoreSupplier(storagePropertiesSupplier);
    }

    @Bean
    public Transformers transformers(EventStoreTransformationRepository repository,
                                     ContextEventIteratorFactory iteratorFactory,
                                     TransformationEntryStoreSupplier transformationEntryStoreSupplier) {
        return new LocalTransformers(iteratorFactory::createFrom, repository, transformationEntryStoreSupplier);
    }

    @Bean
    public Transformations transformations(EventStoreTransformationRepository repository) {
        return new JpaTransformations(repository);
    }

    @Bean
    public LocalTransformationProgressStore localTransformationProgressStore(
            EventStoreTransformationProgressRepository repository) {
        return new JpaLocalTransformationProgressStore(repository);
    }

    @Bean
    public LocalTransformationApplyExecutor localTransformationApplier(
            TransformationEntryStoreSupplier transformationEntryStoreSupplier,
            LocalTransformationProgressStore localTransformationProgressStore,
            LocalEventStoreTransformer localEventStoreTransformer) {
        return new DefaultLocalTransformationApplyExecutor(transformationEntryStoreSupplier,
                                                           localTransformationProgressStore,
                                                           localEventStoreTransformer);
    }

    @Bean
    @ConditionalOnMissingBean(TransformationApplyExecutor.class)
    public TransformationApplyExecutor transformationApplier(
            LocalTransformationApplyExecutor localTransformationApplyExecutor) {
        return new StandardTransformationApplyExecutor(localTransformationApplyExecutor);
    }

    @Bean
    public TransformationApplyTask transformationApplyTask(TransformationApplyExecutor applier,
                                                           Transformers transformers,
                                                           Transformations transformations) {
        return new DefaultTransformationApplyTask(applier, transformers, transformations);
    }

    @Bean
    public LocalTransformationRollbackExecutor localTransformationRollbackExecutor(
            LocalEventStoreTransformer localEventStoreTransformer) {
        return new DefaultLocalTransformationRollbackExecutor(localEventStoreTransformer);
    }

    @Bean
    public TransformationRollbackExecutor transformationRollbackExecutor(
            LocalTransformationRollbackExecutor localTransformationRollbackExecutor) {
        return new StandardTransformationRollbackExecutor(localTransformationRollbackExecutor);
    }

    @Bean
    public TransformationRollBackTask transformationRollBackTask(
            TransformationRollbackExecutor transformationRollbackExecutor,
            Transformations transformations,
            Transformers transformers) {
        return new DefaultTransformationRollBackTask(transformationRollbackExecutor, transformations, transformers);
    }

    @Bean
    public LocalTransformationCancelExecutor localTransformationCancelExecutor(
            TransformationEntryStoreSupplier transformationEntryStoreSupplier) {
        return new DefaultLocalTransformationCancelExecutor(
                context -> transformationEntryStoreSupplier.supply(context)
                                                           .flatMap(TransformationEntryStore::delete));
    }

    @Bean
    public TransformationCancelExecutor transformationCancelExecutor(
            LocalTransformationCancelExecutor localTransformationCancelExecutor) {
        return new StandardTransformationCancelExecutor(localTransformationCancelExecutor);
    }

    @Bean
    public TransformationCancelTask transformationCancelTask(TransformationCancelExecutor transformationCancelExecutor,
                                                             Transformers transformers,
                                                             Transformations transformations) {
        return new DefaultTransformationCancelTask(transformationCancelExecutor, transformers, transformations);
    }

    @Bean
    @ConditionalOnMissingBean(EventStoreTransformationService.class)
    public EventStoreTransformationService eventStoreTransformationService(Transformers transformers,
                                                                           Transformations transformations,
                                                                           TransformationRollBackTask transformationRollBackTask,
                                                                           TransformationApplyTask transformationApplyTask,
                                                                           TransformationCancelTask transformationCancelTask) {
        return new DefaultEventStoreTransformationService(transformers,
                                                          transformations,
                                                          transformationRollBackTask,
                                                          transformationApplyTask,
                                                          transformationCancelTask);
    }

    @Bean
    @ConditionalOnMissingBean(ApplyCompletedConsumer.class)
    public ApplyCompletedConsumer applyCompletedConsumer(Transformers transformers) {
        return (context, transformationId) -> transformers.transformerFor(context)
                                                          .flatMap(transformer -> transformer.markApplied(
                                                                  transformationId));
    }

    @Bean
    @ConditionalOnMissingBean(ApplyCompletedConsumer.class)
    public RollbackCompletedConsumer rollbackCompletedConsumer(Transformers transformers) {
        return (context, transformationId) -> transformers.transformerFor(context)
                                                          .flatMap(transformer -> transformer.markRolledBack(
                                                                  transformationId));
    }
}
