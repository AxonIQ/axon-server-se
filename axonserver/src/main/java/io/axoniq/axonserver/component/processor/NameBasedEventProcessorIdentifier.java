package io.axoniq.axonserver.component.processor;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Implementation of {@link EventProcessorIdentifier} the use the name of the processor as unique identifier.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public final class NameBasedEventProcessorIdentifier implements EventProcessorIdentifier {

    private final String name;

    /**
     * Creates an instance of {@link NameBasedEventProcessorIdentifier} based on the specified processor name.
     *
     * @param name the name of the event processor.
     */
    public NameBasedEventProcessorIdentifier(@Nonnull String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NameBasedEventProcessorIdentifier that = (NameBasedEventProcessorIdentifier) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
