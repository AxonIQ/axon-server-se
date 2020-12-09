/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import org.springframework.stereotype.Controller;

import java.io.InputStream;
import java.util.Map;

/**
 * Implements the {@link ExtensionController} for Axon Server Standard Edition. Forwards all requests to the
 * {@link OsgiController} directly.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Controller
public class DefaultExtensionController implements ExtensionController {

    private final OsgiController osgiController;
    private final ExtensionConfigurationManager configurationManager;

    public DefaultExtensionController(OsgiController osgiController,
                                      ExtensionConfigurationManager configurationManager) {
        this.osgiController = osgiController;
        this.configurationManager = configurationManager;
    }

    @Override
    public Iterable<ExtensionInfo> listExtensions() {
        return osgiController.listBundles();
    }

    @Override
    public void uninstallExtension(BundleInfo bundleInfo) {
        osgiController.uninstallExtension(bundleInfo);
    }

    @Override
    public void addExtension(String fileName, InputStream inputStream) {
        osgiController.addExtension(fileName, inputStream);
    }

    @Override
    public Iterable<ExtensionProperty> listProperties(BundleInfo bundleInfo) {
        return configurationManager.configuration(bundleInfo);
    }

    @Override
    public void updateConfiguration(BundleInfo bundleInfo, Map<String, String> properties) {
        configurationManager.updateConfiguration(bundleInfo, properties);
    }
}
