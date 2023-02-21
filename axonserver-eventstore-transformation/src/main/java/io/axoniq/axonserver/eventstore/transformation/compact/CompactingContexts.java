package io.axoniq.axonserver.eventstore.transformation.compact;


/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface CompactingContexts extends Iterable<CompactingContexts.CompactingContext> {

    interface CompactingContext {

        String compactionId();

        String context();
    }
}
