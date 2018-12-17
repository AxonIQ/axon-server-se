package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.TopologyEvents;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;

import java.util.function.Supplier;

/**
 * Author: marc
 */
public class ClusterEvents {
    private ClusterEvents() {
    }


    @KeepNames
    public static class AxonServerInstanceConnected extends TopologyEvents.TopologyBaseEvent {

        private final RemoteConnection remoteConnection;

        public AxonServerInstanceConnected(RemoteConnection remoteConnection){
            super(false);
            this.remoteConnection = remoteConnection;
        }

        public RemoteConnection getRemoteConnection() {
            return remoteConnection;
        }
    }

    @KeepNames
    public static class AxonServerInstanceDisconnected extends TopologyEvents.TopologyBaseEvent {
        private final String nodeName;

        public AxonServerInstanceDisconnected(String nodeName) {
            super(false);
            this.nodeName = nodeName;
        }

        public String getNodeName() {
            return nodeName;
        }
    }


    /**
     * Author: marc
     */
    @KeepNames
    public static class LeaderStepDown extends TopologyEvents.TopologyBaseEvent {

        private final String contextName;

        public LeaderStepDown(String contextName, boolean forwarded) {
            super(forwarded);
            this.contextName = contextName;
        }

        public String getContextName() {
            return contextName;
        }
    }

    /**
     * Author: marc
     */
    @KeepNames
    public static class LeaderConfirmation extends TopologyEvents.TopologyBaseEvent {

        private final String context;
        private final String node;

        public LeaderConfirmation(String context, String node, boolean forwarded) {
            super(forwarded);
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

    /**
     * Author: marc
     */
    @KeepNames
    public static class BecomeLeader extends TopologyEvents.TopologyBaseEvent {

        private final String context;
        private final Supplier<EntryIterator> unappliedEntries;

        public BecomeLeader(String context, Supplier<EntryIterator> unappliedEntries) {
            super(false);
            this.context = context;
            this.unappliedEntries = unappliedEntries;
        }

        public String getContext() {
            return context;
        }

        public Supplier<EntryIterator> getUnappliedEntries() {
            return unappliedEntries;
        }
    }

    @KeepNames
    public static class AxonServerNodeDeleted extends TopologyEvents.TopologyBaseEvent {
        private final String node;

        public AxonServerNodeDeleted(String name) {
            super(false);
            this.node =name;
        }

        public String node() {
            return node;
        }
    }
}
