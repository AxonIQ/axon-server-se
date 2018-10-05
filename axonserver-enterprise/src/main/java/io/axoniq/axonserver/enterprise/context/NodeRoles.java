package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;

/**
 * Author: marc
 */
@KeepNames
public class NodeRoles {

    private String name;
    private boolean storage;
    private boolean messaging;

    public NodeRoles() {
    }

    public NodeRoles(ContextClusterNode contextClusterNode) {
        this(contextClusterNode.getClusterNode().getName(), contextClusterNode.isMessaging(), contextClusterNode.isStorage());
    }

    public NodeRoles(String name, boolean messaging, boolean storage) {
        this.name = name;
        this.messaging = messaging;
        this.storage = storage;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isStorage() {
        return storage;
    }

    public void setStorage(boolean storage) {
        this.storage = storage;
    }

    public boolean isMessaging() {
        return messaging;
    }

    public void setMessaging(boolean messaging) {
        this.messaging = messaging;
    }

    @Override
    public String toString() {
        return "NodeRoles{" +
                "name='" + name + '\'' +
                ", storage=" + storage +
                ", messaging=" + messaging +
                '}';
    }
}
