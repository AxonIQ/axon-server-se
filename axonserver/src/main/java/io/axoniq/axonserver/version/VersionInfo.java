/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.version;

/**
 * Data object containing the product name and version.
 *
 * @author Marc Gathier
 * @since 4.2.4
 */
public class VersionInfo {

    private final String productName;
    private final String version;
    private final String licenseType;

    public VersionInfo(String productName, String version) {
        this(productName, version, null);
    }

    public VersionInfo(String productName, String version, String licenseType) {
        this.productName = productName;
        this.version = version;
        this.licenseType = licenseType;
    }

    public String getProductName() {
        return productName;
    }

    public String getVersion() {
        return version;
    }

    public String getLicenseType() {
        return licenseType;
    }
}
