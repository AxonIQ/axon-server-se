package io.axoniq.axonserver.enterprise.storage.spanner;

import com.google.auth.Credentials;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Specific properties for Spanner storage.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@ConfigurationProperties(prefix = "axoniq.axonserver.storage.spanner")
@Configuration
public class StorageProperties {

    private String projectId;
    private String instance;

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public Credentials getCredentials() {
        return new Credentials() {
            @Override
            public String getAuthenticationType() {
                return null;
            }

            @Override
            public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
                return null;
            }

            @Override
            public boolean hasRequestMetadata() {
                return false;
            }

            @Override
            public boolean hasRequestMetadataOnly() {
                return false;
            }

            @Override
            public void refresh() throws IOException {

            }
        };
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }
}
