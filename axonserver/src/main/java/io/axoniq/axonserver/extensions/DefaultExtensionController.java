/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.rest.ExtensionPropertyGroup;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.stereotype.Controller;

import java.io.InputStream;
import java.util.List;
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

    private final ExtensionPackageManager extensionPackageManager;
    private final ExtensionConfigurationManager configurationManager;
    private final ExtensionContextManager extensionContextManager;
    private final ExtensionConfigurationSerializer extensionConfigurationSerializer;

    public DefaultExtensionController(ExtensionPackageManager extensionPackageManager,
                                      ExtensionConfigurationManager configurationManager,
                                      ExtensionContextManager extensionContextManager,
                                      ExtensionConfigurationSerializer extensionConfigurationSerializer) {
        this.extensionPackageManager = extensionPackageManager;
        this.configurationManager = configurationManager;
        this.extensionContextManager = extensionContextManager;
        this.extensionConfigurationSerializer = extensionConfigurationSerializer;
    }

    @Override
    public Iterable<ExtensionInfo> listExtensions() {
        return extensionPackageManager.listExtensions();
    }

    @Override
    public void uninstallExtension(ExtensionKey extensionKey) {
        extensionPackageManager.uninstallExtension(extensionKey);
    }

    @Override
    public ExtensionKey addExtension(String fileName, InputStream inputStream) {
        return extensionPackageManager.addExtension(fileName, inputStream).getKey();
    }

    @Override
    public List<ExtensionPropertyGroup> listProperties(ExtensionKey extensionKey, String context) {
        List<ExtensionPropertyGroup> definedProperties = configurationManager.configuration(extensionKey);
        extensionContextManager.getStatus(Topology.DEFAULT_CONTEXT,
                                          extensionKey.getSymbolicName(),
                                          extensionKey.getVersion())
                               .ifPresent(extensionStatus -> ExtensionPropertyUtils.setValues(definedProperties,
                                                                                              extensionStatus
                                                                                                      .getConfiguration(),
                                                                                              extensionConfigurationSerializer));
        return definedProperties;
    }

    @Override
    public void updateConfiguration(ExtensionKey extensionKey, String context,
                                    Map<String, Map<String, Object>> properties) {
        properties = ExtensionPropertyUtils.validateProperties(properties,
                                                               listProperties(extensionKey, Topology.DEFAULT_CONTEXT));
        extensionContextManager.updateConfiguration(Topology.DEFAULT_CONTEXT,
                                                    extensionKey.getSymbolicName(),
                                                    extensionKey.getVersion(),
                                                    properties);
    }

    @Override
    public void updateExtensionStatus(ExtensionKey extensionKey, String context, boolean active) {
        extensionContextManager.updateStatus(Topology.DEFAULT_CONTEXT,
                                             extensionKey.getSymbolicName(),
                                             extensionKey.getVersion(),
                                             active);
    }

    @Override
    public void unregisterExtensionForContext(ExtensionKey extensionKey, String context) {
        extensionContextManager.removeForContext(extensionKey, Topology.DEFAULT_CONTEXT);
    }
}
