package io.axoniq.axonserver.config;

import io.axoniq.axonserver.KeepNames;

/**
 * Configuration properties related to Axon Server Access Control.
 *
 * @author Marc Gathier
 */
@KeepNames
public class AccessControlConfiguration {

    /**
     * Indicates that access control is enabled for the server.
     */
    private boolean enabled;
    /**
     * Timeout for authenticated tokens.
     */
    private long cacheTtl = 30000;
    /**
     * Token used to authenticate Axon Server instances in a cluster (only used by Enterprise Edition)
     */
    private String internalToken;
    /**
     * Token to be used by client applications connecting to Axon Server (only used by Standard Edition)
     */
    private String token;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getCacheTtl() {
        return cacheTtl;
    }

    public void setCacheTtl(long cacheTtl) {
        this.cacheTtl = cacheTtl;
    }

    public String getInternalToken() {
        return internalToken;
    }

    public void setInternalToken(String internalToken) {
        this.internalToken = internalToken;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
