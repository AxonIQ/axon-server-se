/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.interceptor.ExtensionEnabledEvent;
import io.axoniq.axonserver.rest.ExtensionPropertyGroup;
import org.osgi.framework.Bundle;
import org.osgi.service.cm.Configuration;
import org.osgi.service.metatype.AttributeDefinition;
import org.osgi.service.metatype.MetaTypeInformation;
import org.osgi.service.metatype.MetaTypeService;
import org.osgi.service.metatype.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages the configuration of extensions.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class ExtensionConfigurationManager {

    private final Logger logger = LoggerFactory.getLogger(ExtensionConfigurationManager.class);

    private final OsgiController osgiController;

    public ExtensionConfigurationManager(OsgiController osgiController) {
        this.osgiController = osgiController;
    }

    @EventListener
    @Order(0)
    public void on(ExtensionEnabledEvent extensionEnabledEvent) {
        if (extensionEnabledEvent.enabled()) {
            updateConfiguration(extensionEnabledEvent.extension(),
                                extensionEnabledEvent.context(),
                                extensionEnabledEvent.configuration());
        }
    }

    /**
     * Updates the configuration of an extension.
     *
     * @param bundleInfo the name and version of the extension
     * @param properties the new properties
     */
    public void updateConfiguration(ExtensionKey bundleInfo, String context,
                                    Map<String, Map<String, Object>> properties) {
//        Bundle bundle = osgiController.getBundle(bundleInfo);
        Set<ConfigurationListener> configurationListeners = osgiController.getConfigurationListeners(bundleInfo);

        configurationListeners.forEach(listener -> listener.updated(context, properties.get(listener.id())));
//        ConfigurationAdmin configurationAdmin = osgiController.get(ConfigurationAdmin.class)
//                                                              .orElseThrow(() -> new MessagingPlatformException(
//                                                                      ErrorCode.OTHER,
//                                                                      "ConfigurationAdmin not found"));
//
//        MetaTypeInformation info = metaTypeService.getMetaTypeInformation(bundle);
//        if (info != null) {
//            for (String pid : info.getPids()) {
//                try {
//                    Configuration configuration = configurationAdmin.getConfiguration(pid, bundle.getLocation());
//                    logger.debug("{}/{}}: {} old properties {}",
//                                 bundle.getSymbolicName(),
//                                 bundle.getVersion(),
//                                 pid,
//                                 configuration.getProperties());
//
//                    ObjectClassDefinition objectClassDefinition = info
//                            .getObjectClassDefinition(pid, null);
//
//
//                    Dictionary<String, Object> updatedConfiguration = currentConfiguration(configuration);
//                    updateWithProvidedProperties(properties.getOrDefault(objectClassDefinition.getID(),
//                                                                         properties.getOrDefault("*",
//                                                                                                 Collections
//                                                                                                         .emptyMap())),
//                                                 updatedConfiguration,
//                                                 context
//                                                 );
//                    configuration.update(updatedConfiguration);
//
//                    logger.info("{}/{}: new properties {}", bundle.getSymbolicName(), bundle.getVersion(),
//                                configurationAdmin.getConfiguration(pid).getProperties());
//                } catch (IOException ioException) {
//                    logger.warn("Configuration update failed", ioException);
//                }
//            }
//        }
    }

    private void updateWithProvidedProperties(Map<String, Object> properties,
                                              Dictionary<String, Object> updatedConfiguration,
                                              String context) {
        properties.forEach((key, value) -> {
            updatedConfiguration.put(keyForContext(context, key), value);
        });
    }

    private String keyForContext(String context, String key) {
        return context + "." + key;
    }

    private Dictionary<String, Object> currentConfiguration(Configuration configuration) {
        Dictionary<String, Object> currentConfiguration = new Hashtable<>();
        if (configuration.getProperties() != null) {
            for (Enumeration<String> keys = configuration.getProperties().keys(); keys.hasMoreElements(); ) {
                String key = keys.nextElement();
                currentConfiguration.put(key, configuration.getProperties().get(key));
            }
        }
        return currentConfiguration;
    }

    /**
     * @param bundleInfo name and version of the bundle
     * @return list of properties that can be set for the extension
     */
    public List<ExtensionPropertyGroup> configuration(ExtensionKey bundleInfo) {
        Bundle bundle = osgiController.getBundle(bundleInfo);
        if (bundle == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Bundle not found");
        }


        MetaTypeService metaTypeService = osgiController.get(MetaTypeService.class)
                                                        .orElseThrow(() -> new MessagingPlatformException(ErrorCode.OTHER,
                                                                                                          "MetaTypeService not found"));

//        ConfigurationAdmin configurationAdmin = osgiController.get(ConfigurationAdmin.class)
//                                                              .orElseThrow(() -> new MessagingPlatformException(
//                                                                      ErrorCode.OTHER,
//                                                                      "ConfigurationAdmin not found"));

        MetaTypeInformation info = metaTypeService.getMetaTypeInformation(bundle);
        List<ExtensionPropertyGroup> extensionProperties = new ArrayList<>();
        for (String pid : info.getPids()) {
            try {
//                Configuration configuration = configurationAdmin.getConfiguration(pid, bundle.getLocation());
                ObjectClassDefinition objectClassDefinition = info
                        .getObjectClassDefinition(pid, null);
                List<ExtensionProperty> properties = new LinkedList<>();
                for (AttributeDefinition attributeDefinition : objectClassDefinition.getAttributeDefinitions(
                        ObjectClassDefinition.ALL)) {
//                    Object value = configuration.getProperties() != null ? configuration.getProperties().get(
//                            keyForContext(context,attributeDefinition.getID())) : null;
                    properties.add(new ExtensionProperty(attributeDefinition, null));
                }
                extensionProperties.add(new ExtensionPropertyGroup(objectClassDefinition.getID(),
                                                                   objectClassDefinition.getName(),
                                                                   properties));
            } catch (Exception ioException) {
                logger.warn("Failed to read configuration for {}", pid, ioException);
            }
        }
        return extensionProperties;
    }
}
