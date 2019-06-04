package io.axoniq.axonserver.enterprise.storage.spanner;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Specific properties for Spanner storage.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@ConfigurationProperties(prefix = "axoniq.axonserver.storage.spanner")
@Configuration
public class StorageProperties {

    /**
     * Google cloud project identification.
     */
    private String projectId;
    /**
     * Google spanner instance
     */
    private String instance;

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }
}
