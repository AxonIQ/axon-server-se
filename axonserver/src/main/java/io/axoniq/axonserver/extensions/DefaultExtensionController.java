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

import java.io.File;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final ExtensionStatusManager extensionStatusManager;
    private final ExtensionConfigurationSerializer extensionConfigurationSerializer;

    public DefaultExtensionController(OsgiController osgiController,
                                      ExtensionConfigurationManager configurationManager,
                                      ExtensionStatusManager extensionStatusManager,
                                      ExtensionConfigurationSerializer extensionConfigurationSerializer) {
        this.osgiController = osgiController;
        this.configurationManager = configurationManager;
        this.extensionStatusManager = extensionStatusManager;
        this.extensionConfigurationSerializer = extensionConfigurationSerializer;
    }

    @Override
    public Iterable<ExtensionInfo> listExtensions() {
        Set<ExtensionKey> extensions = new HashSet<>();
        osgiController.listExtensions().forEach(b -> extensions
                .add(new ExtensionKey(b.getSymbolicName(), b.getVersion().toString())));

        return extensionStatusManager.listExtensions(extensions);
    }

    @Override
    public void uninstallExtension(ExtensionKey extensionKey) {
        extensionStatusManager.uninstall(extensionKey);
        osgiController.uninstallExtension(extensionKey);
    }

    @Override
    public ExtensionKey addExtension(String fileName, InputStream inputStream) {
        ExtensionKey extensionKey = osgiController.addExtension(fileName, inputStream);
        extensionStatusManager.publishConfiguration(extensionKey);
        return extensionKey;
    }

    @Override
    public List<ExtensionPropertyGroup> listProperties(ExtensionKey extensionKey, String context) {
        List<ExtensionPropertyGroup> definedProperties = configurationManager.configuration(extensionKey);
        extensionStatusManager.getStatus(Topology.DEFAULT_CONTEXT,
                                         extensionKey.getSymbolicName(),
                                         extensionKey.getVersion())
                              .ifPresent(extensionStatus -> setValues(definedProperties,
                                                                      extensionStatus.getConfiguration()));
        return definedProperties;
    }

    private void setValues(List<ExtensionPropertyGroup> definedProperties, String serializedConfiguration) {
        Map<String, Map<String, Object>> configuration = extensionConfigurationSerializer.deserialize(
                serializedConfiguration);
        if (configuration != null) {
            definedProperties.forEach(propertyGroup -> {
                Map<String, Object> configurationForGroup = configuration.get(propertyGroup.getId());
                propertyGroup.getProperties().forEach(prop -> prop
                        .setValue(configurationForGroup.getOrDefault(prop.getId(), prop.getDefaultValue())));
            });
        }
    }

    @Override
    public void updateConfiguration(ExtensionKey extensionKey, String context,
                                    Map<String, Map<String, Object>> properties) {
        File location = osgiController.getLocation(extensionKey);
        extensionStatusManager.updateConfiguration(Topology.DEFAULT_CONTEXT,
                                                   extensionKey.getSymbolicName(),
                                                   extensionKey.getVersion(),
                                                   location.getName(),
                                                   properties);
    }

    @Override
    public void updateExtensionStatus(ExtensionKey extensionKey, String context, boolean active) {
        File location = osgiController.getLocation(extensionKey);
        if (active) {
            osgiController.updateStatus(extensionKey, true);
        }
        extensionStatusManager.updateStatus(Topology.DEFAULT_CONTEXT,
                                            extensionKey.getSymbolicName(),
                                            extensionKey.getVersion(),
                                            location.getName(),
                                            active);
    }

    @Override
    public void unregisterExtensionForContext(ExtensionKey extensionKey, String context) {
        extensionStatusManager.removeForContext(extensionKey, Topology.DEFAULT_CONTEXT);
    }
}
