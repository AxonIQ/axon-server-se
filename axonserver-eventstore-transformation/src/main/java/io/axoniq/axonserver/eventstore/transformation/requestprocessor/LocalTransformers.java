package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class LocalTransformers implements Transformers {

    private static final Logger logger = LoggerFactory.getLogger(LocalTransformers.class);

    private final Map<String, Mono<ContextTransformer>> transformers = new ConcurrentHashMap<>();
    private final EventStoreTransformationRepository transformationRepository;
    private final Function<String, EventProvider> eventProviderFactory;
    private final TransformationEntryStoreSupplier entryStoreSupplier;
    private final EventStoreStateStore eventStoreStateStore;


    public LocalTransformers(Function<String, EventProvider> eventProviderFactory,
                             EventStoreTransformationRepository eventStoreTransformationRepository,
                             TransformationEntryStoreSupplier entryStoreSupplier,
                             EventStoreStateStore eventStoreStateStore) {
        this.eventProviderFactory = eventProviderFactory;
        this.transformationRepository = eventStoreTransformationRepository;
        this.entryStoreSupplier = entryStoreSupplier;
        this.eventStoreStateStore = eventStoreStateStore;
    }

    @Override
    public Mono<ContextTransformer> transformerFor(String context) {
        logger.info("Invoking transformerFor {}", context);
        return transformers.computeIfAbsent(context, c ->
                                   entryStoreSupplier.supply(context)
                                                     .doFirst(() -> logger.info("Getting the event store by the supplier for context {}.",
                                                                                context))
                                                     .doOnNext(tes -> logger.info("Loaded Transformation Entry Store for context {}.",
                                                                                  context))
                                                     .map(transformationEntryStore -> new LocalContextTransformationStore(
                                                             context,
                                                             transformationRepository,
                                                             transformationEntryStore))
                                                     .<ContextTransformer>map(store -> {
                                                         EventProvider eventProvider = eventProviderFactory.apply(context);
                                                         TransformationStateConverter converter = new ContextTransformationStateConverter(
                                                                 eventProvider);
                                                         return new SequentialContextTransformer(context, store,
                                                                                                 eventStoreStateStore,
                                                                                                 converter);
                                                     }).cache())
                           .doOnSubscribe(s -> logger.info("Subscribed to transformerFor."));
    }
}
