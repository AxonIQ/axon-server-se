/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Get information about the current AxonServer instance.
 * @author Marc Gathier
 */
public interface SystemInfoProvider {

    /**
     * Returns the HTTP port number used to connect to AxonServer.
     * @return the port number
     */
    default int getPort() {
        return 8080;
    }

    /**
     * Returns the hostname of the current node.
     * @return the host name
     *
     * @throws UnknownHostException if the server cannot determine its hostname.
     */
    default String getHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    /**
     * Checks if the AxonServer is running on a Windows machine.
     * @return true if runs on windows.
     */
    default boolean javaOnWindows() {
        String os = System.getProperty("os.name").toLowerCase();
        return os.startsWith("win");
    }

    /**
     * Checks the Java version for module support.
     * @return true if the Java version is not 1.8.
     */
    default boolean javaWithModules() {
        String version = System.getProperty("java.version");
        return ! version.startsWith("1.8");
    }

}
