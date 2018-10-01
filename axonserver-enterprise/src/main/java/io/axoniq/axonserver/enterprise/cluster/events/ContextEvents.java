package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.context.NodeRoles;

import java.util.List;

/**
 * Author: marc
 */
public class ContextEvents {
    @KeepNames
    public abstract static class BaseContextEvent {
        private final String name;
        private final boolean proxied;

        BaseContextEvent(String name, boolean proxied) {
            this.name = name;
            this.proxied = proxied;
        }

        public String getName() {
            return name;
        }

        public boolean isProxied() {
            return proxied;
        }
    }

    @KeepNames
    public static class ContextCreated extends BaseContextEvent {
        private final List<NodeRoles> nodes;

        public ContextCreated(String name, List<NodeRoles> nodes, boolean proxied) {
            super(name, proxied);
            this.nodes = nodes;
        }

        public List<NodeRoles> getNodes() {
            return nodes;
        }
    }

    @KeepNames
    public static class ContextDeleted extends BaseContextEvent {
        public ContextDeleted(String name, boolean proxied) {
            super(name, proxied);
        }
    }

    @KeepNames
    public static class NodeAddedToContext extends BaseContextEvent {
        private final NodeRoles node;

        public NodeAddedToContext(String name, NodeRoles node, boolean proxied) {
            super(name, proxied);
            this.node = node;
        }

        public NodeRoles getNode() {
            return node;
        }
    }

    @KeepNames
    public static class NodeDeletedFromContext extends BaseContextEvent {
        private final String node;

        public NodeDeletedFromContext(String name, String node, boolean proxied) {
            super(name, proxied);
            this.node = node;
        }

        public String getNode() {
            return node;
        }
    }

}
