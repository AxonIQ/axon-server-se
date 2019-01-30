package io.axoniq.axonserver.enterprise.jpa;

import io.axoniq.axonserver.KeepNames;

import java.io.Serializable;
import java.util.Objects;
import javax.persistence.AssociationOverride;
import javax.persistence.AssociationOverrides;
import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.PreRemove;
import javax.persistence.Table;
import javax.persistence.Transient;

/**
 * @author Marc Gathier
 */
@Entity
@Table(name = "CONTEXT_CLUSTER_NODE")
@AssociationOverrides({
        @AssociationOverride(name = "key.context",
                joinColumns = @JoinColumn(name = "CONTEXT_NAME")),
        @AssociationOverride(name = "key.clusterNode",
                joinColumns = @JoinColumn(name = "CLUSTER_NODE_NAME")) })
public class ContextClusterNode implements Serializable {

    @EmbeddedId
    private Key key;

    private boolean storage;
    private boolean messaging;

    public ContextClusterNode() {
    }

    public ContextClusterNode(Context context, ClusterNode clusterNode) {
        this.key = new Key(context, clusterNode);
        context.addClusterNode(this);
        clusterNode.addContext(this);
    }

    public Key getKey() {
        return key;
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

    @Transient
    public Context getContext() {
        return key.context;
    }

    @Transient
    public ClusterNode getClusterNode() {
        return key.clusterNode;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContextClusterNode that = (ContextClusterNode) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @PreRemove
    public void preDelete() {
        key.clusterNode.remove(this);
        key.context.remove(this);
    }

    @Embeddable
    @KeepNames
    public static class Key implements Serializable {
        @ManyToOne
        private Context context;
        @ManyToOne
        private ClusterNode clusterNode;

        public Key() {
        }

        public Key(Context context, ClusterNode clusterNode) {
            this.context = context;
            this.clusterNode = clusterNode;
        }

        public Context getContext() {
            return context;
        }

        public ClusterNode getClusterNode() {
            return clusterNode;
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
            return Objects.equals(context, key.context) &&
                    Objects.equals(clusterNode, key.clusterNode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(context, clusterNode);
        }
    }

}
