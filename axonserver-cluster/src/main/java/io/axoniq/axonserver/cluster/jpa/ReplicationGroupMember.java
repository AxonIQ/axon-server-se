package io.axoniq.axonserver.cluster.jpa;

import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;

import java.io.Serializable;
import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;

/**
 * An entity describing a member of a raft group
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Entity
@IdClass(ReplicationGroupMember.Key.class)
@Table(name = "rg_member")
public class ReplicationGroupMember {

    @Id
    private String groupId;
    @Id
    private String nodeId;
    private String host;
    private int port;
    private String nodeName;
    private Role role;

    // deletion of the member of the raft group has been initiated
    private boolean pendingDelete;

    public ReplicationGroupMember(String groupId, Node node) {
        this.groupId = groupId;
        this.nodeId = node.getNodeId();
        this.host = node.getHost();
        this.port = node.getPort();
        this.nodeName = node.getNodeName();
        this.role = node.getRole();
    }

    public ReplicationGroupMember() {
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public Node asNode() {
        return Node.newBuilder()
                   .setNodeId(getNodeId())
                   .setNodeName(getNodeName())
                   .setHost(getHost())
                   .setPort(getPort())
                   .setRole(getRole())
                   .build();
    }


    public void setRole(Role role) {
        this.role = role;
    }

    public Role getRole() {
        return RoleUtils.getOrDefault(role);
    }

    public boolean isPendingDelete() {
        return pendingDelete;
    }

    public void setPendingDelete(boolean pendingDelete) {
        this.pendingDelete = pendingDelete;
    }

    public static class Key implements Serializable {

        private String groupId;
        private String nodeId;

        public Key() {
        }

        public Key(String groupId, String nodeId) {
            this.groupId = groupId;
            this.nodeId = nodeId;
        }

        public String getGroupId() {
            return groupId;
        }

        public String getNodeId() {
            return nodeId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Objects.equals(groupId, key.groupId) &&
                    Objects.equals(nodeId, key.nodeId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupId, nodeId);
        }
    }
}
