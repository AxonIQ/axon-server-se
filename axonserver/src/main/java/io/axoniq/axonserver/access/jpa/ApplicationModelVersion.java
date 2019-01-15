package io.axoniq.axonserver.access.jpa;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Author: marc
 */
@Entity
public class ApplicationModelVersion {
    @Id
    private String applicationName;
    private long version;

    public ApplicationModelVersion() {
    }

    public ApplicationModelVersion(String applicationName, long version) {
        this.applicationName = applicationName;
        this.version = version;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }
}
