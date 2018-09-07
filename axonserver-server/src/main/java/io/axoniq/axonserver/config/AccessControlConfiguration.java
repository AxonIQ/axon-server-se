package io.axoniq.axonserver.config;

import io.axoniq.axonserver.KeepNames;

/**
 * Author: marc
 */
@KeepNames
public class AccessControlConfiguration {
    private boolean enabled;
    private long cacheTtl = 30000;
    private String internalToken;
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
