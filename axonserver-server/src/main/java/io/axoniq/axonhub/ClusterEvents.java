package io.axoniq.axonhub;

import io.axoniq.axonhub.grpc.internal.RemoteConnection;
import io.axoniq.axonhub.internal.grpc.ContextRole;

import java.util.List;

/**
 * Author: marc
 */
public class ClusterEvents {

    public abstract static class ClusterBaseEvent {
        private final boolean forwarded;

        protected ClusterBaseEvent(boolean forwarded) {
            this.forwarded = forwarded;
        }

        public boolean isForwarded() {
            return forwarded;
        }
    }

    @KeepNames
    public static class ApplicationConnected extends ClusterBaseEvent {
        private final String context;
        private final String componentName;
        private final String client;
        private final String proxy;

        public ApplicationConnected(String context, String componentName, String client, String proxy) {
            super(proxy != null);
            this.context = context;
            this.componentName = componentName;
            this.client = client;
            this.proxy = proxy;
        }
        public ApplicationConnected(String context, String componentName, String client) {
            this(context, componentName, client, null);
        }

        public String getComponentName() {
            return componentName;
        }

        public String getClient() {
            return client;
        }

        public String getContext() {
            return context;
        }

        public String getProxy() {
            return proxy;
        }

        public boolean isProxied() {
            return isForwarded();
        }
    }

    @KeepNames
    public static class ApplicationDisconnected extends ClusterBaseEvent {
        private final String context;
        private final String componentName;
        private final String client;
        private final String proxy;

        public ApplicationDisconnected(String context, String componentName, String client, String proxy) {
            super(proxy != null);
            this.context = context;
            this.componentName = componentName;
            this.client = client;
            this.proxy = proxy;
        }

        public ApplicationDisconnected(String context,
                                       String componentName, String client
        ) {
            this(context, componentName, client, null);
        }

        public String getComponentName() {
            return componentName;
        }

        public String getClient() {
            return client;
        }

        public String getContext() {
            return context;
        }

        public String getProxy() {
            return proxy;
        }

        public boolean isProxied() {
            return isForwarded();
        }

    }

    @KeepNames
    public static class AxonHubInstanceConnected extends ClusterBaseEvent {

        private final RemoteConnection remoteConnection;
        private final long modelVersion;
        private final List<ContextRole> contextsList;
        private final List<io.axoniq.axonhub.internal.grpc.NodeInfo> nodesList;

        public AxonHubInstanceConnected(RemoteConnection remoteConnection, long modelVersion,
                                        List<ContextRole> contextsList,
                                        List<io.axoniq.axonhub.internal.grpc.NodeInfo> nodesList){
            super(false);
            this.remoteConnection = remoteConnection;
            this.modelVersion = modelVersion;
            this.contextsList = contextsList;
            this.nodesList = nodesList;
        }

        public RemoteConnection getRemoteConnection() {
            return remoteConnection;
        }

        public long getModelVersion() {
            return modelVersion;
        }

        public List<ContextRole> getContextsList() {
            return contextsList;
        }

        public List<io.axoniq.axonhub.internal.grpc.NodeInfo> getNodesList() {
            return nodesList;
        }
    }

    @KeepNames
    public static class AxonHubInstanceDisconnected extends ClusterBaseEvent {
        private final String nodeName;

        public AxonHubInstanceDisconnected(String nodeName) {
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
    public static class MasterStepDown extends ClusterBaseEvent {

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
    public static class MasterDisconnected extends ClusterBaseEvent{

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
    public static class MasterConfirmation extends ClusterBaseEvent {

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
    public static class BecomeMaster extends ClusterBaseEvent {

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
    public static class CoordinatorStepDown extends ClusterBaseEvent {

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
    public static class CoordinatorConfirmation extends ClusterBaseEvent {

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
    public static class BecomeCoordinator extends ClusterBaseEvent {

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
