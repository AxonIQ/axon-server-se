package io.axoniq.axonserver.config;

import io.axoniq.axonserver.KeepNames;

/**
 * @author Marc Gathier
 */
@KeepNames
public class SslConfiguration {
    private boolean enabled;
    private String certChainFile;
    private String privateKeyFile;
    private String internalCertChainFile;
    private String internalTrustManagerFile;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getCertChainFile() {
        return certChainFile;
    }

    public void setCertChainFile(String certChainFile) {
        this.certChainFile = certChainFile;
    }

    public String getPrivateKeyFile() {
        return privateKeyFile;
    }

    public void setPrivateKeyFile(String privateKeyFile) {
        this.privateKeyFile = privateKeyFile;
    }

    public String getInternalCertChainFile() {
        if( internalCertChainFile == null) return certChainFile;
        return internalCertChainFile;
    }

    public void setInternalCertChainFile(String internalCertChainFile) {
        this.internalCertChainFile = internalCertChainFile;
    }

    public String getInternalTrustManagerFile() {
        return internalTrustManagerFile;
    }

    public void setInternalTrustManagerFile(String internalTrustManagerFile) {
        this.internalTrustManagerFile = internalTrustManagerFile;
    }
}
