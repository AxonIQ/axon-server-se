/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.exception.ErrorCode;
import org.springframework.validation.Errors;

import java.io.File;

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

    public void validate(Errors errors) {
        if (enabled) {
            validFile(new File(certChainFile), "ssl.cert-chain-file", errors);
            validFile(new File(privateKeyFile), "ssl.private-key-file", errors);

            if (internalCertChainFile != null) {
                validFile(new File(internalCertChainFile), "ssl.internal-cert-chain-file", errors);
            }
            if (internalPrivateKeyFile != null) {
                validFile(new File(internalPrivateKeyFile), "ssl.internal-private-key-file", errors);
            }
            if (internalTrustManagerFile != null) {
                validFile(new File(internalTrustManagerFile), "ssl.internal-trust-manager-file", errors);
            }
        }
    }

    private void validFile(File file, String property, Errors errors) {
        if (!file.exists()) {
            errors.rejectValue(property, ErrorCode.VALIDATION_FAILED.getCode(), "File not found");
            return;
        }
        if (!file.canRead()) {
            errors.rejectValue(property, ErrorCode.VALIDATION_FAILED.getCode(), "Cannot read file");
        }
    }
}
