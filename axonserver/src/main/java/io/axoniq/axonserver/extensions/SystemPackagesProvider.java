/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class SystemPackagesProvider {

    private final String systemPackages;
    private static final String[] systemPackageNames = {
            "io.axoniq.axonserver.extensions",
            "io.axoniq.axonserver.extensions.interceptor",
            "io.axoniq.axonserver.extensions.transform",
            "io.axoniq.axonserver.extensions.hook",
            "io.axoniq.axonserver.grpc",
            "io.axoniq.axonserver.grpc.command",
            "io.axoniq.axonserver.grpc.control",
            "io.axoniq.axonserver.grpc.event",
            "io.axoniq.axonserver.grpc.query"};

    public SystemPackagesProvider(String version) {
        this.systemPackages = Arrays.stream(systemPackageNames)
                                    .map(s -> String.format("%s;version=\"%s\"", s, version))
                                    .collect(
                                            Collectors.joining(","));
    }

    public String getSystemPackages() {
        return systemPackages;
    }
}
