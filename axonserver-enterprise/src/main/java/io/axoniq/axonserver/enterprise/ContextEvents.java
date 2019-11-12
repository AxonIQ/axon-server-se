package io.axoniq.axonserver.enterprise;


import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.applicationevents.TopologyEvents;

/**
 * @author Marc Gathier
 */
public class ContextEvents {

    @KeepNames
    public static class ContextCreated extends TopologyEvents.TopologyBaseEvent {
        private final String context;

        public ContextCreated(String context) {
            super(false);
            this.context = context;
        }

        public String getContext() {
            return context;
        }
    }

    @KeepNames
    public static class ContextUpdated extends TopologyEvents.TopologyBaseEvent {

        private final String context;

        public ContextUpdated(String context) {
            super(false);
            this.context = context;
        }

        public String getContext() {
            return context;
        }
    }

    @KeepNames
    public static class AdminContextDeleted extends TopologyEvents.TopologyBaseEvent {
        private final String context;

        public AdminContextDeleted(String context) {
            super(false);
            this.context = context;
        }

        public String getContext() {
            return context;
        }
    }

    @KeepNames
    public static class ContextDeleted extends TopologyEvents.TopologyBaseEvent {

        private final String context;
        private final boolean preserveEventStore;

        public ContextDeleted(String context, boolean preserveEventStore) {
            super(false);
            this.context = context;
            this.preserveEventStore = preserveEventStore;
        }

        public String getContext() {
            return context;
        }

        public boolean preserveEventStore() {
            return preserveEventStore;
        }
    }

    /**
     * Event published when there is an intent to remove a node from a context.
     */
    @KeepNames
    public static class DeleteNodeFromContextRequested extends TopologyEvents.TopologyBaseEvent {

        private final String context;
        private final String node;

        /**
         * Constructor for the event.
         *
         * @param context the context where the node will be deleted
         * @param node    the node that will be deleted from the context
         */
        public DeleteNodeFromContextRequested(
                String context, String node) {
            super(false);
            this.context = context;
            this.node = node;
        }

        public String getContext() {
            return context;
        }

        public String getNode() {
            return node;
        }
    }
}
