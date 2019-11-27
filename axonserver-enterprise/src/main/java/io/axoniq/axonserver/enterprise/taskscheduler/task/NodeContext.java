package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;

/**
 * Payload for the delete node from context jobs.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@KeepNames
public class NodeContext {

    private String node;
    private String context;
    private boolean preserveEventStore;

    /**
     * Constructor for de-serializing
     */
    public NodeContext() {
    }

    /**
     * Constructor to initialize the object.
     *
     * @param node               the node to delete
     * @param context            the context to delete the node from
     * @param preserveEventStore true if the event store should not be removed from the node
     */
    public NodeContext(String node, String context, boolean preserveEventStore) {
        this.node = node;
        this.context = context;
        this.preserveEventStore = preserveEventStore;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public boolean isPreserveEventStore() {
        return preserveEventStore;
    }

    public void setPreserveEventStore(boolean preserveEventStore) {
        this.preserveEventStore = preserveEventStore;
    }
}
