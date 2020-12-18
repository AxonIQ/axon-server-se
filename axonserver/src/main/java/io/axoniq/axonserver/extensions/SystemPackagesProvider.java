/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.jar.Manifest;

/**
 * @author Marc Gathier
 */
public class SystemPackagesProvider {

    private static final Logger logger = LoggerFactory.getLogger(SystemPackagesProvider.class);

    public String getSystemPackages() {
        try {
            List<String> exports = new LinkedList<>();
            Enumeration<URL> manifestEnumeration = Thread.currentThread().getContextClassLoader().getResources(
                    "META-INF/MANIFEST.MF");
            while (manifestEnumeration.hasMoreElements()) {
                URL manifestUrl = manifestEnumeration.nextElement();
                if (export(manifestUrl.toString())) {
                    try (InputStream manifestInputStream = manifestUrl.openStream()) {
                        Manifest manifest = new Manifest(manifestInputStream);
                        String name = manifest.getMainAttributes().getValue("Export-Package");
                        if (name != null) {
                            logger.warn("Adding exports from {} to system packages path",
                                        manifest.getMainAttributes()
                                                .getValue("Bundle-SymbolicName"));
                            exports.add(manifest.getMainAttributes()
                                                .getValue("Export-Package"));
                        }
                    }
                }
            }
            return String.join(",", exports.toArray(new String[0]));
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

    private boolean export(String manifestUrl) {
        return manifestUrl.contains("org.osgi") ||
                manifestUrl.contains("org.apache.felix.metatype") ||
                manifestUrl.contains("org.apache.felix.configadmin") ||
                manifestUrl.contains("axon-server-extension-api") ||
                manifestUrl.contains("axonserver-extension-api");
    }
}
