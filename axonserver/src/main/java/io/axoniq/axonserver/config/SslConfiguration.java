/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.grpc.util.AdvancedTlsX509TrustManager;

/**
 * Configuration properties for SSL (TLS) settings.
 *
 * @author Marc Gathier
 */
public class SslConfiguration {

    /**
     * Indicates that SSL is enabled and gRPC servers should start in SSL mode.
     */
    private boolean enabled;
    /**
     * File containing the full certificate chain.
     */
    private String certChainFile;
    /**
     * File containing the private key.
     */
    private String privateKeyFile;
    /**
     * File containing the full certificate chain to be used in internal communication between Axon Server nodes.
     */
    private String internalCertChainFile;
    /**
     * Trusted certificates for verifying the other AxonServer's certificate.
     */
    private String internalTrustManagerFile;
    /**
     * File containing the private key to be used in internal communication between Axon Server nodes.
     */
    private String internalPrivateKeyFile;

    /**
     * Certificate verification level for the internal communication between Axon Server nodes (possible values
     * CERTIFICATE_AND_HOST_NAME_VERIFICATION, CERTIFICATE_ONLY_VERIFICATION or INSECURELY_SKIP_ALL_VERIFICATION).
     */
    private AdvancedTlsX509TrustManager.Verification trustManagerVerification =
            AdvancedTlsX509TrustManager.Verification.CERTIFICATE_AND_HOST_NAME_VERIFICATION;

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
        if (internalCertChainFile == null) {
            return certChainFile;
        }
        return internalCertChainFile;
    }

    public void setInternalCertChainFile(String internalCertChainFile) {
        this.internalCertChainFile = internalCertChainFile;
    }

    public String getInternalPrivateKeyFile() {
        if (internalPrivateKeyFile == null) {
            return privateKeyFile;
        }
        return internalPrivateKeyFile;
    }

    public void setInternalPrivateKeyFile(String internalPrivateKeyFile) {
        this.internalPrivateKeyFile = internalPrivateKeyFile;
    }

    public String getInternalTrustManagerFile() {
        return internalTrustManagerFile;
    }

    public void setInternalTrustManagerFile(String internalTrustManagerFile) {
        this.internalTrustManagerFile = internalTrustManagerFile;
    }

    public AdvancedTlsX509TrustManager.Verification getTrustManagerVerification() {
        return trustManagerVerification;
    }

    public void setTrustManagerVerification(AdvancedTlsX509TrustManager.Verification trustManagerVerification) {
        this.trustManagerVerification = trustManagerVerification;
    }
}
