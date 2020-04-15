package io.axoniq.axonserver.applicationevents;

/**
 * Marker interface needed to identify internal Axon Server events that
 * must be published across all instances of Axon Server into the current cluster.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public interface AxonServerEvent {

}
