/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Marc Gathier
 */
public class BundleInfo {

    private final String symbolicName;
    private final String version;

    @JsonCreator
    public BundleInfo(@JsonProperty("symbolicName") String symbolicName, @JsonProperty("version") String version) {
        this.symbolicName = symbolicName;
        this.version = version;
    }

    public String getSymbolicName() {
        return symbolicName;
    }

    public String getVersion() {
        return version;
    }
}
