package io.axoniq.axonserver.enterprise.component.connection.rule;

import org.jetbrains.annotations.NotNull;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
public interface ConnectionValue extends Comparable<ConnectionValue> {

    double weight();

    @Override
    default int compareTo(@NotNull ConnectionValue o) {
        return Double.compare(this.weight(), o.weight());
    }
}
