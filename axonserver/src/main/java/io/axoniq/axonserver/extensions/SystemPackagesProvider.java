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
                                            Collectors.joining(","))
                + ",org.apache.felix.metatype;version=\"1.2.0\";uses:=\"org.osgi.framework,org.osgi.service.metatype\""
                + ",org.osgi.service.metatype;version=\"1.4.0\";uses:=\"org.osgi.framework\""
                + ",org.osgi.service.log;version=\"1.3.0\""
                + ",org.apache.felix.cm;version=\"1.2.0\""
                + ",org.apache.felix.cm.file;version=\"1.1.0\";uses:=\"org.apache.felix.cm,org.osgi.framework\""
                + ",org.osgi.service.cm;version=\"1.6.0\";uses:=\"org.osgi.framework\""
                + ",com.google.protobuf;version=\"3.12.0\"";
    }

    public String getSystemPackages() {
        return systemPackages;
    }
}
