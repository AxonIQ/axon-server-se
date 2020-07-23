package io.axoniq.axonserver.cluster.jpa;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Marc Gathier
 */
@Entity
@Table(name = "rg_replication_group")
public class ReplicationGroup {

    @Id
    private String replicationGroupId;

    private String name;

    public String getReplicationGroupId() {
        return replicationGroupId;
    }

    public void setReplicationGroupId(String replicationGroupId) {
        this.replicationGroupId = replicationGroupId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
