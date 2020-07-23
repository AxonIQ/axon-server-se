package io.axoniq.axonserver.enterprise.jpa;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * Stores information about a Context.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@Entity(name = "Context")
@Table(name = "adm_context")
public class AdminContext implements Serializable {

    @Id
    private String name;

    @ManyToOne
    @JoinColumn(name = "replication_group_id")
    private AdminReplicationGroup replicationGroup;

    @Column(name = "CHANGE_PENDING")
    private Boolean changePending;

    @Column(name = "PENDING_SINCE")
    @Temporal(TemporalType.TIMESTAMP)
    private Date pendingSince;

    @Column(name = "META_DATA")
    @Lob
    private String metaData;

    public AdminContext() {
    }

    public AdminContext(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AdminContext context1 = (AdminContext) o;
        return Objects.equals(name, context1.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }


    public boolean isChangePending() {
        return changePending != null && changePending;
    }

    public void changePending(Boolean changePending) {
        this.changePending = changePending;
        if(changePending != null && changePending) {
            pendingSince = new Date();
        } else {
            pendingSince = null;
        }
    }

    public Date getPendingSince() {
        return pendingSince;
    }

    @Override
    public String toString() {
        return "Context{" +
                "name='" + name + '\'' +
                '}';
    }

    public String getMetaData() {
        return metaData;
    }

    public void setMetaData(String metaData) {
        this.metaData = metaData;
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

    public AdminReplicationGroup getReplicationGroup() {
        return replicationGroup;
    }

    public void setReplicationGroup(AdminReplicationGroup replicationGroup) {
        this.replicationGroup = replicationGroup;
    }
}
