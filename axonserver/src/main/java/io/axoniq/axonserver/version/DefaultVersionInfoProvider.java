/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.version;

import org.springframework.beans.factory.annotation.Value;

/**
 * Provides information on the current version of the product.
 *
 * @author Marc Gathier
 * @since 4.2.4
 */
public class DefaultVersionInfoProvider implements VersionInfoProvider {

    @Value("${info.app.name:Axon Server}")
    private String applicationName;
    @Value("${info.app.version:Unknown}")
    private String defaultVersion;

    @Override
    public VersionInfo get() {
        return new VersionInfo(applicationName, defaultVersion);
    }
}
