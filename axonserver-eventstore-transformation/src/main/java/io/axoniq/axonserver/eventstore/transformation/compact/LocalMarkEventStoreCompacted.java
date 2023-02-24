package io.axoniq.axonserver.eventstore.transformation.compact;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class LocalMarkEventStoreCompacted implements MarkEventStoreCompacted {

    private static final Logger logger = LoggerFactory.getLogger(LocalMarkEventStoreCompacted.class);

    private final Transformers transformers;

    public LocalMarkEventStoreCompacted(Transformers transformers) {
        this.transformers = transformers;
    }

    @Override
    public Mono<Void> markCompacted(String compactionId, String context) {
        return transformers.transformerFor(context)
                           .doFirst(() -> logger.info("Marking event store context {} compacted.", context))
                           .flatMap(transformer -> transformer.markCompacted(compactionId))
                           .doOnSuccess(unused -> logger.info("Event store context {} marked compacted.", context))
                           .doOnError(error -> logger.warn("Error marking context {} as compacted.", context, error));
    }
}
