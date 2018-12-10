package io.axoniq.axonserver.cluster.jpa;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.io.Serializable;
import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;

/**
 * Author: marc
 */
@Entity
@IdClass(JpaRaftGroupNode.Key.class)
public class JpaRaftGroupNode {
    @Id
    private String groupId;
    @Id
    private String nodeId;
    private String host;
    private int port;

    public JpaRaftGroupNode(String groupId, Node node) {
        this.groupId = groupId;
        this.nodeId = node.getNodeId();
        this.host = node.getHost();
        this.port = node.getPort();
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
