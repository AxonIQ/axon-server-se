package io.axoniq.axonserver.enterprise;


import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.applicationevents.AxonServerEvent;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.grpc.cluster.Role;

/**
 * @author Marc Gathier
 */
public class ContextEvents {

    @KeepNames
    public static class ContextCreated extends TopologyEvents.TopologyBaseEvent {

        private final String context;
        private final String replicationGroup;
        private final Role role;
        private final long defaultFirstEventToken;
        private final long defaultFirstSnapshotToken;

        public ContextCreated(String context, String replicationGroup, Role role, long defaultFirstEventToken,
                              long defaultFirstSnapshotToken) {
            super(false);
            this.context = context;
            this.replicationGroup = replicationGroup;
            this.role = role;
            this.defaultFirstEventToken = defaultFirstEventToken;
            this.defaultFirstSnapshotToken = defaultFirstSnapshotToken;
        }

        public String context() {
            return context;
        }

        public String replicationGroup() {
            return replicationGroup;
        }

        public Role role() {
            return role;
        }

        public long defaultFirstEventToken() {
            return defaultFirstEventToken;
        }

        public long defaultFirstSnapshotToken() {
            return defaultFirstSnapshotToken;
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

        public String context() {
            return context;
        }

        public boolean preserveEventStore() {
            return preserveEventStore;
        }
    }

    @KeepNames
    public static class ContextPreDelete implements AxonServerEvent {

        private final String context;

        public ContextPreDelete(String context) {
            this.context = context;
        }

        public String context() {
            return context;
        }
    }
}
