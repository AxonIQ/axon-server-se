package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides and caches the correct {@link ContextTransformer} for each context.
 */
public class LocalTransformers implements Transformers {

    private static final Logger logger = LoggerFactory.getLogger(LocalTransformers.class);

    private final Map<String, Mono<ContextTransformer>> transformers = new ConcurrentHashMap<>();
    private final EventStoreTransformationRepository transformationRepository;
    private final TransformationEntryStoreProvider entryStoreSupplier;
    private final EventStoreStateStore eventStoreStateStore;
    private final PlatformTransactionManager transactionManager;


    /**
     * Constructs an instance based on the specified parameters.
     *
     * @param eventStoreTransformationRepository used to persist the transformation state
     * @param entryStoreSupplier                 used to get the transformation actions store
     * @param eventStoreStateStore               used to persist the event store state
     * @param transactionManager                 to manage the controlDB transaction when multiple tables are updated
     */
    public LocalTransformers(EventStoreTransformationRepository eventStoreTransformationRepository,
                             TransformationEntryStoreProvider entryStoreSupplier,
                             EventStoreStateStore eventStoreStateStore,
                             PlatformTransactionManager transactionManager) {
        this.transformationRepository = eventStoreTransformationRepository;
        this.entryStoreSupplier = entryStoreSupplier;
        this.eventStoreStateStore = eventStoreStateStore;
        this.transactionManager = transactionManager;
    }

    @Override
    public Mono<ContextTransformer> transformerFor(String context) {
        logger.info("Invoking transformerFor {}", context);
        return transformers.computeIfAbsent(context, c -> contextTransformerMono(c).cache());
    }

    @Override
    public Mono<Void> clean(String context) {
        return transformerFor(context)
                .flatMap(ContextTransformer::clean)
                .then(Mono.fromRunnable(() -> transformers.remove(context)));
    }

    private Mono<ContextTransformer> contextTransformerMono(String context) {
        return Mono.fromSupplier(() -> new LocalContextTransformationStore(context,
                                                                           transformationRepository,
                                                                           entryStoreSupplier))
                   .map(store -> {
                       TransformationStateConverter converter = new ContextTransformationStateConverter();
                       ContextTransformer transactionalContextTransformer = new TransactionalContextTransformer(context,
                                                                                                                store,
                                                                                                                transactionManager,
                                                                                                                eventStoreStateStore,
                                                                                                                converter);
                       return new SequentialContextTransformer(transactionalContextTransformer);
                   });
    }
}
