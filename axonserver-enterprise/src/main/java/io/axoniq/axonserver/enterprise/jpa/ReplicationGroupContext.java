package io.axoniq.axonserver.enterprise.jpa;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

/**
 * @author Marc Gathier
 */
@Entity
@Table(name = "rg_context")
public class ReplicationGroupContext {

    @Id
    private String name;

    @Column(name = "META_DATA")
    @Lob
    private String metaData;

    @Column(name = "replication_group_name")
    private String replicationGroupName;

    public ReplicationGroupContext() {
    }

    public ReplicationGroupContext(String name, String replicationGroupName) {
        this.name = name;
        this.replicationGroupName = replicationGroupName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMetaData() {
        return metaData;
    }

    public void setMetaData(String metaData) {
        this.metaData = metaData;
    }

    public String getReplicationGroupName() {
        return replicationGroupName;
    }

    public void setReplicationGroupName(String replicationGroupName) {
        this.replicationGroupName = replicationGroupName;
    }

    public void setMetaDataMap(Map<String, String> metaDataMap) {
        this.metaData = null;
        try {
            this.metaData = new ObjectMapper().writeValueAsString(metaDataMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> getMetaDataMap() {
        if (metaData == null) {
            return Collections.emptyMap();
        }

        try {
            return (Map<String, String>) new ObjectMapper().readValue(metaData, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyMap();
        }
    }
}
