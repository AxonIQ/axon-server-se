package io.axoniq.axonserver.enterprise;

import io.axoniq.axonserver.TopologyEvents;
import io.axoniq.platform.KeepNames;

/**
 * Author: marc
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
    public static class ContextDeleted extends TopologyEvents.TopologyBaseEvent {
        private final String context;

        public ContextDeleted(String context) {
            super(false);
            this.context = context;
        }

        public String getContext() {
            return context;
        }
    }
}
