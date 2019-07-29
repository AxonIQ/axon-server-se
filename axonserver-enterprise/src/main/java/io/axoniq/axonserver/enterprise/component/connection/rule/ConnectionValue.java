package io.axoniq.axonserver.enterprise.component.connection.rule;

import org.jetbrains.annotations.NotNull;

/**
 * Represent the suitability of the connection of a specified client with one server node in the cluster.
 * Highest value represents a better choice.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public interface ConnectionValue extends Comparable<ConnectionValue> {

    /**
     * Returns the value of the connection of a specified client with one server node in the cluster
     *
     * @return the value of the connection
     */
    double weight();

    @Override
    default int compareTo(@NotNull ConnectionValue o) {
        return Double.compare(this.weight(), o.weight());
    }
}
