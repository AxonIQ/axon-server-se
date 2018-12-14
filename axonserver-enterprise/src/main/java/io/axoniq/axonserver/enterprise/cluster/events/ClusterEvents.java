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
    public static class MasterStepDown extends TopologyEvents.TopologyBaseEvent {

        private final String contextName;

        public MasterStepDown(String contextName, boolean forwarded) {
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
    public static class MasterDisconnected extends TopologyEvents.TopologyBaseEvent{

        private final String contextName;

        public MasterDisconnected(String contextName, boolean forwarded) {
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
    public static class MasterConfirmation extends TopologyEvents.TopologyBaseEvent {

        private final String context;
        private final String node;

        public MasterConfirmation(String context, String node, boolean forwarded) {
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
    public static class BecomeMaster extends TopologyEvents.TopologyBaseEvent {

        private final String context;
        private final Supplier<EntryIterator> unappliedEntries;

        public BecomeMaster(String context, Supplier<EntryIterator> unappliedEntries) {
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
    public static class CoordinatorStepDown extends TopologyEvents.TopologyBaseEvent {

        private final String context;

        public CoordinatorStepDown(String context, boolean forwarded) {
            super(forwarded);
            this.context = context;
        }

        public String context() {
            return context;
        }

    }

    @KeepNames
    public static class CoordinatorConfirmation extends TopologyEvents.TopologyBaseEvent {

        private final String node;
        private final String context;

        public CoordinatorConfirmation(String node, String context, boolean forwarded) {
            super(forwarded);
            this.node = node;
            this.context = context;
        }

        public String node() {
            return node;
        }

        public String context() {
            return context;
        }
    }

    @KeepNames
    public static class BecomeCoordinator extends TopologyEvents.TopologyBaseEvent {

        private final String node;
        private final String context;

        public BecomeCoordinator(String node, String context, boolean forwarded) {
            super(forwarded);
            this.node = node;
            this.context = context;
        }

        public String node() {
            return node;
        }

        public String context() {
            return context;
        }
    }

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
