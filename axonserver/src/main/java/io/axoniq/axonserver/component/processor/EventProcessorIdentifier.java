package io.axoniq.axonserver.component.processor;

/**
 * Semantic identifier for Event Processor.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public interface EventProcessorIdentifier {

    /**
     * Returns the name of the event processor.
     *
     * @return the name of the event processor.
     */
    String name();
}
