package io.axoniq.axonserver.enterprise.storage.jdbc;

/**
 * Strategy to determine if the sync action should actually store the data on this node.
 *
 * @author Marc Gathier
 */
public interface SyncStrategy {

    /**
     * Checks if this node should store the events.
     * @return true if this node should store the event.
     */
    boolean storeOnNode();
}
