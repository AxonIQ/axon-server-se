package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.ModelVersion;
import io.axoniq.axonserver.grpc.internal.NodeInfo;

import java.util.List;

/**
 * Author: marc
 */
public class ClusterEvents {


    @KeepNames
    public static class AxonServerInstanceConnected extends TopologyEvents.TopologyBaseEvent {

        private final RemoteConnection remoteConnection;
        private final long generation;
        private final List<ModelVersion> modelVersionsList;
        private final List<ContextRole> contextsList;
        private final List<io.axoniq.axonserver.grpc.internal.NodeInfo> nodesList;

        public AxonServerInstanceConnected(RemoteConnection remoteConnection,
                                           long generation,
                                           List<ModelVersion> modelVersionsList,
                                           List<ContextRole> contextsList,
                                           List<NodeInfo> nodesList){
            super(false);
            this.remoteConnection = remoteConnection;
            this.generation = generation;
            this.modelVersionsList = modelVersionsList;
            this.contextsList = contextsList;
            this.nodesList = nodesList;
        }

        public RemoteConnection getRemoteConnection() {
            return remoteConnection;
        }

        public long getModelVersion(String name) {
            return modelVersionsList.stream()
                                    .filter(mv -> mv.getName().equals(name))
                                    .findFirst()
                                    .map(ModelVersion::getValue)
                                    .orElse(0L);
        }

        public List<ContextRole> getContextsList() {
            return contextsList;
        }

        public List<io.axoniq.axonserver.grpc.internal.NodeInfo> getNodesList() {
            return nodesList;
        }

        public long getGeneration() {
            return generation;
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
        private final String oldMaster;

        public MasterDisconnected(String contextName, String oldMaster, boolean forwarded) {
            super(forwarded);
            this.contextName = contextName;
            this.oldMaster = oldMaster;
        }

        public String getContextName() {
            return contextName;
        }

        public String oldMaster() {
            return oldMaster;
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
        private final String node;

        public BecomeMaster(String context, String node, boolean forwarded) {
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

    @KeepNames
    public static class AxonServerNodeDeleted extends TopologyEvents.TopologyBaseEvent {
        private final String node;
        private final long generation;

        public AxonServerNodeDeleted(String name, long generation) {
            super(false);
            this.node =name;
            this.generation = generation;
        }

        public String node() {
            return node;
        }

        public long getGeneration() {
            return generation;
        }
    }

    @KeepNames
    public static class ClusterUpdatedNotification extends TopologyEvents.TopologyBaseEvent {

        public ClusterUpdatedNotification() {
            super(false);
        }
    }

    @KeepNames
    public static class InternalServerReady extends TopologyEvents.TopologyBaseEvent {

        public InternalServerReady() {
            super(false);
        }
    }
}
