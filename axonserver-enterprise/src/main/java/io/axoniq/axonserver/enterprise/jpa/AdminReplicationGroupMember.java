package io.axoniq.axonserver.enterprise.jpa;


import io.axoniq.axonserver.grpc.cluster.Role;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * @author Marc Gathier
 */
@Table(name = "adm_replication_group_member")
@Entity
public class AdminReplicationGroupMember {

    @Id
    @GeneratedValue
    private Long id;

    @ManyToOne
    @JoinColumn(name = "replication_group_id")
    private AdminReplicationGroup replicationGroup;

    @ManyToOne
    @JoinColumn(name = "cluster_node_name")
    private ClusterNode clusterNode;

    private String clusterNodeLabel;

    private Role role;
    private Boolean pendingDelete;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public AdminReplicationGroup getReplicationGroup() {
        return replicationGroup;
    }

    public void setReplicationGroup(AdminReplicationGroup replicationGroup) {
        this.replicationGroup = replicationGroup;
    }

    public ClusterNode getClusterNode() {
        return clusterNode;
    }

    public void setClusterNode(ClusterNode clusterNode) {
        this.clusterNode = clusterNode;
    }

    public String getClusterNodeLabel() {
        return clusterNodeLabel;
    }

    public void setClusterNodeLabel(String clusterNodeLabel) {
        this.clusterNodeLabel = clusterNodeLabel;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public void setPendingDelete(boolean pendingDelete) {
        this.pendingDelete = pendingDelete;
    }

    public boolean isPendingDelete() {
        return pendingDelete != null && pendingDelete;
    }
}
