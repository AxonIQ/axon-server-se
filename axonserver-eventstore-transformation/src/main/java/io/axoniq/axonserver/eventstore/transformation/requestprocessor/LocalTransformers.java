package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class LocalTransformers implements Transformers {

    private final Map<String, Mono<ContextTransformer>> transformers = new ConcurrentHashMap<>();
    private final EventStoreTransformationRepository transformationRepository;
    private final Function<String, EventProvider> eventProviderFactory;
    private final TransformationEntryStoreSupplier entryStoreSupplier;


    public LocalTransformers(Function<String, EventProvider> eventProviderFactory,
                             EventStoreTransformationRepository eventStoreTransformationRepository,
                             TransformationEntryStoreSupplier entryStoreSupplier) {
        this.eventProviderFactory = eventProviderFactory;
        this.transformationRepository = eventStoreTransformationRepository;
        this.entryStoreSupplier = entryStoreSupplier;
    }

    @Override
    public Mono<ContextTransformer> transformerFor(String context) {

        return transformers.computeIfAbsent(context, c ->
                entryStoreSupplier.supply(context)
                                  .map(transformationEntryStore -> new LocalContextTransformationStore(
                                          context,
                                          transformationRepository,
                                          transformationEntryStore))
                                  .<ContextTransformer>map(store -> {
                                      EventProvider eventProvider = eventProviderFactory.apply(context);
                                      TransformationStateConverter converter = new ContextTransformationStateConverter(eventProvider);
                                      return new SequentialContextTransformer(context, store, converter);
                                  }).cache());
    }
}
