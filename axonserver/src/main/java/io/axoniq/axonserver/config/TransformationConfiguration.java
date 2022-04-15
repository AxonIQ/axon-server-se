package io.axoniq.axonserver.config;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.ApplyCompletedConsumer;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultEventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultTransformationApplyTask;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.DefaultTransformationRollBackTask;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationRepository;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.JpaTransformations;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.LocalTransformers;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.RollbackCompletedConsumer;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.SegmentBasedTransformationEntryStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationApplier;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationApplyTask;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationRollBackTask;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import io.axoniq.axonserver.filestorage.AppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.impl.BaseAppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import io.axoniq.axonserver.localstorage.AutoCloseableEventProvider;
import io.axoniq.axonserver.localstorage.ContextEventIteratorFactory;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.transformation.DefaultLocalTransformationApplier;
import io.axoniq.axonserver.localstorage.transformation.LocalEventStoreTransformer;
import io.axoniq.axonserver.localstorage.transformation.LocalTransformationApplier;
import io.axoniq.axonserver.localstorage.transformation.LocalTransformationProgressStore;
import io.axoniq.axonserver.localstorage.transformation.StandardTransformationApplier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
    public Transformers transformers(EventStoreTransformationRepository repository,
                                     ContextEventIteratorFactory iteratorFactory) {
        return new LocalTransformers(iteratorFactory::createFrom, repository);
    }

    @Bean
    public Transformations transformations(EventStoreTransformationRepository repository) {
        return new JpaTransformations(repository);
    }

    @Bean
    public TransformationEntryStore transformationEntryStore(StorageProperties storageProperties) {
        AppendOnlyFileStore fileStore = new BaseAppendOnlyFileStore(storageProperties, "transformation");
        return new SegmentBasedTransformationEntryStore(fileStore);
    }

    @Bean
    public LocalTransformationProgressStore localTransformationProgressStore() {
        return ne
    }

    @Bean
    public LocalTransformationApplier localTransformationApplier(TransformationEntryStore transformationEntryStore,
                                                                 LocalTransformationProgressStore localTransformationProgressStore,
                                                                 LocalEventStoreTransformer localEventStoreTransformer) {
        return new DefaultLocalTransformationApplier(transformationEntryStore,
                                                     localTransformationProgressStore,
                                                     localEventStoreTransformer);
    }

    @Bean
    @ConditionalOnMissingBean(TransformationApplier.class)
    public TransformationApplier transformationApplier(LocalTransformationApplier localTransformationApplier) {
        return new StandardTransformationApplier(localTransformationApplier);
    }

    @Bean
    public TransformationApplyTask transformationApplyTask(TransformationApplier applier,
                                                           Transformers transformers,
                                                           Transformations transformations) {
        return new DefaultTransformationApplyTask(applier, transformers, transformations);
    }

    @Bean
    public TransformationRollBackTask transformationRollBackTask() {
        return new DefaultTransformationRollBackTask();
    }

    @Bean
    @ConditionalOnMissingBean(EventStoreTransformationService.class)
    public EventStoreTransformationService eventStoreTransformationService(Transformers transformers,
                                                                           Transformations transformations,
                                                                           TransformationRollBackTask transformationRollBackTask,
                                                                           TransformationApplyTask transformationApplyTask) {
        return new DefaultEventStoreTransformationService(transformers,
                                                          transformations,
                                                          transformationRollBackTask,
                                                          transformationApplyTask);
    }

    @Bean
    @ConditionalOnMissingBean(ApplyCompletedConsumer.class)
    public ApplyCompletedConsumer applyCompletedConsumer(Transformers transformers) {
        return (context, transformationId) -> transformers.transformerFor(context).markApplied(transformationId);
    }

    @Bean
    @ConditionalOnMissingBean(ApplyCompletedConsumer.class)
    public RollbackCompletedConsumer rollbackCompletedConsumer(Transformers transformers) {
        return (context, transformationId) -> transformers.transformerFor(context).markRolledBack(transformationId);
    }
}
