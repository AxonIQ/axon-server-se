package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.TopologyEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.internal.grpc.ContextRole;
import io.axoniq.axonserver.internal.grpc.ModelVersion;
import io.axoniq.axonserver.internal.grpc.NodeInfo;

import java.util.List;

/**
 * Author: marc
 */
public class ClusterEvents {


    @KeepNames
    public static class AxonServerInstanceConnected extends TopologyEvents.TopologyBaseEvent {

        private final RemoteConnection remoteConnection;
        private final List<ModelVersion> modelVersionsList;
        private final List<ContextRole> contextsList;
        private final List<io.axoniq.axonserver.internal.grpc.NodeInfo> nodesList;

        public AxonServerInstanceConnected(RemoteConnection remoteConnection,
                                           List<ModelVersion> modelVersionsList,
                                           List<ContextRole> contextsList,
                                           List<NodeInfo> nodesList){
            super(false);
            this.remoteConnection = remoteConnection;
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

        public List<io.axoniq.axonserver.internal.grpc.NodeInfo> getNodesList() {
            return nodesList;
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

}
