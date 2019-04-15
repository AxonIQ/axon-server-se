/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utility to retrieve information about the host where AxonServer is running.
 *
 * @author Marc Gathier
 */
@Component
public class DefaultSystemInfoProvider implements SystemInfoProvider {
    private final Environment environment;

    public DefaultSystemInfoProvider(Environment environment) {
        this.environment = environment;
    }

    @Override
    public int getPort() {
        int httpPort;
        String portS = environment.getProperty("server.port", environment.getProperty("internal.server.port", "8080"));
        try {
            httpPort = Integer.valueOf(portS);
        } catch (NumberFormatException nfe) {
            httpPort = 8080;
        }
        return httpPort;
    }

    @Override
    public String getHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
}
