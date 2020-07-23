package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.applicationevents.AxonServerEvent;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.grpc.internal.NodeInfo;

import java.util.function.Supplier;

/**
 * Events published when something happens in the Axon Server cluster.
 * @author Marc Gathier
 * @since 4.0
 */
public class ClusterEvents {
    private ClusterEvents() {
    }

    /**
     * Event on new Axon Server node added to the cluster.
     */
    @KeepNames
    public static class AxonServerNodeConnected {

        private final NodeInfo nodeInfo;

        public AxonServerNodeConnected(NodeInfo nodeInfo) {
            this.nodeInfo = nodeInfo;
        }

        public NodeInfo getNodeInfo() {
            return nodeInfo;
        }
    }

    /**
     * Event on known Axon Server node connects to current node.
     */
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

        public String getNodeName() {
            return remoteConnection.getClusterNode().getName();
        }
    }

    /**
     * Event on known Axon Server node disconnects from current node.
     */
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
     * Event when current node is no longer leader for a replication group.
     */
    @KeepNames
    public static class LeaderStepDown extends TopologyEvents.TopologyBaseEvent {

        private final String replicationGroup;

        public LeaderStepDown(String replicationGroup) {
            super(false);
            this.replicationGroup = replicationGroup;
        }

        public String replicationGroup() {
            return replicationGroup;
        }
    }

    /**
     * Event when there is a change of leader for a replication group.
     */
    @KeepNames
    public static class LeaderConfirmation extends TopologyEvents.TopologyBaseEvent {

        private final String replicationGroup;
        private final String node;

        public LeaderConfirmation(String replicationGroup, String node) {
            super(false);
            this.replicationGroup = replicationGroup;
            this.node = node;
        }

        public String replicationGroup() {
            return replicationGroup;
        }

        public String node() {
            return node;
        }
    }

    /**
     * Event sent by leader to cluster when it becomes leader.
     */
    @KeepNames
    public static class LeaderNotification implements AxonServerEvent {

        private final String replicationGroup;
        private final String node;

        public LeaderNotification(String replicationGroup, String node) {
            this.replicationGroup = replicationGroup;
            this.node = node;
        }

        public String replicationGroup() {
            return replicationGroup;
        }

        public String node() {
            return node;
        }
    }

    /**
     * Event when the current node becomes leader for a replication group.
     */
    @KeepNames
    public static class BecomeLeader extends TopologyEvents.TopologyBaseEvent {

        private final String replicationGroup;
        private final Supplier<EntryIterator> unappliedEntries;

        public BecomeLeader(String replicationGroup, Supplier<EntryIterator> unappliedEntries) {
            super(false);
            this.replicationGroup = replicationGroup;
            this.unappliedEntries = unappliedEntries;
        }

        public String replicationGroup() {
            return replicationGroup;
        }

        public Supplier<EntryIterator> unappliedEntriesSupplier() {
            return unappliedEntries;
        }
    }

    /**
     * Event when current node is no longer leader for a context.
     */
    @KeepNames
    public static class ContextLeaderStepDown extends TopologyEvents.TopologyBaseEvent {

        private final String contextName;

        public ContextLeaderStepDown(String contextName) {
            super(false);
            this.contextName = contextName;
        }

        public String context() {
            return contextName;
        }
    }

    /**
     * Event when there is a change of leader for a context.
     */
    @KeepNames
    public static class ContextLeaderConfirmation extends TopologyEvents.TopologyBaseEvent {

        private final String context;
        private final String node;

        public ContextLeaderConfirmation(String context, String node) {
            super(false);
            this.context = context;
            this.node = node;
        }

        public String context() {
            return context;
        }

        public String node() {
            return node;
        }
    }

    /**
     * Event when the current node becomes leader for a context.
     */
    @KeepNames
    public static class BecomeContextLeader extends TopologyEvents.TopologyBaseEvent {

        private final String context;
        private final Supplier<EntryIterator> unappliedEntries;

        public BecomeContextLeader(String context, Supplier<EntryIterator> unappliedEntries) {
            super(false);
            this.context = context;
            this.unappliedEntries = unappliedEntries;
        }

        public String context() {
            return context;
        }

        public Supplier<EntryIterator> unappliedEntriesSupplier() {
            return unappliedEntries;
        }
    }
    /**
     * Event when an axon server node is deleted from the cluster.
     */
    @KeepNames
    public static class AxonServerNodeDeleted extends TopologyEvents.TopologyBaseEvent {

        private final String node;

        public AxonServerNodeDeleted(String name) {
            super(false);
            this.node = name;
        }

        public String node() {
            return node;
        }
    }

    public static class LicenseUpdated {
        private final byte[] license;

        public LicenseUpdated(byte[] license) {
            this.license = license;
        }

        public byte[] getLicense() {
            return license;
        }
    }
    @KeepNames
    public static class ReplicationGroupUpdated extends TopologyEvents.TopologyBaseEvent {

        private final String replicationGroup;

        public ReplicationGroupUpdated(String replicationGroup) {
            super(false);
            this.replicationGroup = replicationGroup;
        }

        public String replicationGroup() {
            return replicationGroup;
        }
    }

    /**
     * Event published when there is an intent to remove a node from a context.
     */
    @KeepNames
    public static class DeleteNodeFromReplicationGroupRequested extends TopologyEvents.TopologyBaseEvent {

        private final String replicationGroup;
        private final String node;

        /**
         * Constructor for the event.
         *
         * @param replicationGroup the context where the node will be deleted
         * @param node             the node that will be deleted from the context
         */
        public DeleteNodeFromReplicationGroupRequested(
                String replicationGroup, String node) {
            super(false);
            this.replicationGroup = replicationGroup;
            this.node = node;
        }

        public String replicationGroup() {
            return replicationGroup;
        }

        public String node() {
            return node;
        }
    }

    @KeepNames
    public static class ReplicationGroupDeleted extends TopologyEvents.TopologyBaseEvent {

        private final String replicationGroup;
        private final boolean preserveEventStore;

        public ReplicationGroupDeleted(String replicationGroup, boolean preserveEventStore) {
            super(false);
            this.replicationGroup = replicationGroup;
            this.preserveEventStore = preserveEventStore;
        }

        public String replicationGroup() {
            return replicationGroup;
        }

        public boolean preserveEventStore() {
            return preserveEventStore;
        }
    }
}
