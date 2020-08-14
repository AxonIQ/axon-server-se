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
        private final String replicationGroup;
        private final boolean preserveEventStore;

        public ContextDeleted(String context, String replicationGroup, boolean preserveEventStore) {
            super(false);
            this.context = context;
            this.replicationGroup = replicationGroup;
            this.preserveEventStore = preserveEventStore;
        }

        public String context() {
            return context;
        }

        public boolean preserveEventStore() {
            return preserveEventStore;
        }

        public String replicationGroup() {
            return replicationGroup;
        }
    }

    @KeepNames
    public static class ContextPreDelete implements AxonServerEvent {

        private final String context;
        private final String replicationGroup;

        public ContextPreDelete(String context, String replicationGroup) {
            this.context = context;
            this.replicationGroup = replicationGroup;
        }

        public String context() {
            return context;
        }

        public String replicationGroup() {
            return replicationGroup;
        }
    }
}
