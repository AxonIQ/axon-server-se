/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.rest.ExtensionPropertyGroup;
import org.osgi.framework.Bundle;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.metatype.AttributeDefinition;
import org.osgi.service.metatype.MetaTypeInformation;
import org.osgi.service.metatype.MetaTypeService;
import org.osgi.service.metatype.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

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
        osgiController.registerBundleListener(this::setConfiguration);
    }

    /**
     * Updates the configuration of an extension.
     *
     * @param bundleInfo the name and version of the extension
     * @param properties the new properties
     */
    public void updateConfiguration(ExtensionKey bundleInfo, Map<String, Map<String, Object>> properties) {
        Bundle bundle = osgiController.getBundle(bundleInfo);
        mergeConfiguration(bundle, properties);
    }

    private void mergeConfiguration(Bundle bundle, Map<String, Map<String, Object>> properties) {
        MetaTypeService metaTypeService = osgiController.get(MetaTypeService.class)
                                                        .orElseThrow(() -> new MessagingPlatformException(ErrorCode.OTHER,
                                                                                                          "MetaTypeService not found"));

        ConfigurationAdmin configurationAdmin = osgiController.get(ConfigurationAdmin.class)
                                                              .orElseThrow(() -> new MessagingPlatformException(
                                                                      ErrorCode.OTHER,
                                                                      "ConfigurationAdmin not found"));

        MetaTypeInformation info = metaTypeService.getMetaTypeInformation(bundle);
        if (info != null) {
            for (String pid : info.getPids()) {
                try {
                    Configuration configuration = configurationAdmin.getConfiguration(pid, bundle.getLocation());
                    logger.debug("{}/{}}: {} old properties {}",
                                 bundle.getSymbolicName(),
                                 bundle.getVersion(),
                                 pid,
                                 configuration.getProperties());

                    Dictionary<String, Object> updatedConfiguration = new Hashtable<>();
                    Set<String> validIds = processMetaTypeInformation(info, pid, updatedConfiguration);

                    ObjectClassDefinition objectClassDefinition = info
                            .getObjectClassDefinition(pid, null);
                    updateWithCurrentConfiguration(configuration, updatedConfiguration);
                    updateWithProvidedProperties(properties.getOrDefault(objectClassDefinition.getID(),
                                                                         properties.getOrDefault("*",
                                                                                                 Collections
                                                                                                         .emptyMap())),
                                                 updatedConfiguration,
                                                 validIds);
                    configuration.update(updatedConfiguration);

                    logger.info("{}/{}: new properties {}", bundle.getSymbolicName(), bundle.getVersion(),
                                configurationAdmin.getConfiguration(pid).getProperties());
                } catch (IOException ioException) {
                    logger.warn("Configuration update failed", ioException);
                }
            }
        }
    }

    private void updateWithProvidedProperties(Map<String, Object> properties,
                                              Dictionary<String, Object> updatedConfiguration,
                                              Set<String> validIds) {
        properties.forEach((key, value) -> {
            if (validIds.contains(key)) {
                updatedConfiguration.put(key, value);
            } else {
                logger.warn("invalid property {}. valid ids are {}", key, validIds);
            }
        });
    }

    private void updateWithCurrentConfiguration(Configuration configuration,
                                                Dictionary<String, Object> updatedConfiguration) {
        if (configuration.getProperties() != null) {
            for (Enumeration<String> keys = configuration.getProperties().keys(); keys.hasMoreElements(); ) {
                String key = keys.nextElement();
                updatedConfiguration.put(key, configuration.getProperties().get(key));
            }
        }
    }

    @Nonnull
    private Set<String> processMetaTypeInformation(MetaTypeInformation info, String pid,
                                                   Dictionary<String, Object> updatedConfiguration) {
        ObjectClassDefinition objectClassDefinition = info
                .getObjectClassDefinition(pid, null);
        Set<String> validIds = new HashSet<>();
        for (AttributeDefinition attributeDefinition : objectClassDefinition.getAttributeDefinitions(
                ObjectClassDefinition.ALL)) {
            validIds.add(attributeDefinition.getID());
            if (attributeDefinition.getDefaultValue() != null &&
                    attributeDefinition.getDefaultValue().length > 0) {
                if (attributeDefinition.getCardinality() == 0) {
                    updatedConfiguration.put(attributeDefinition.getID(),
                                             attributeDefinition.getDefaultValue()[0]);
                } else {
                    updatedConfiguration.put(attributeDefinition.getID(),
                                             attributeDefinition.getDefaultValue());
                }
            }
        }
        return validIds;
    }

    /**
     * @param bundleInfo name and version of the bundle
     * @return list of properties that can be set/are set for the extension
     */
    public List<ExtensionPropertyGroup> configuration(ExtensionKey bundleInfo) {
        Bundle bundle = osgiController.getBundle(bundleInfo);
        if (bundle == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Bundle not found");
        }

        MetaTypeService metaTypeService = osgiController.get(MetaTypeService.class)
                                                        .orElseThrow(() -> new MessagingPlatformException(ErrorCode.OTHER,
                                                                                                          "MetaTypeService not found"));

        ConfigurationAdmin configurationAdmin = osgiController.get(ConfigurationAdmin.class)
                                                              .orElseThrow(() -> new MessagingPlatformException(
                                                                      ErrorCode.OTHER,
                                                                      "ConfigurationAdmin not found"));

        MetaTypeInformation info = metaTypeService.getMetaTypeInformation(bundle);
        List<ExtensionPropertyGroup> extensionPropertyGroups = new ArrayList<>();
        for (String pid : info.getPids()) {
            try {
                Configuration configuration = configurationAdmin.getConfiguration(pid, bundle.getLocation());
                ObjectClassDefinition objectClassDefinition = info
                        .getObjectClassDefinition(pid, null);
                List<ExtensionProperty> result = new LinkedList<>();
                for (AttributeDefinition attributeDefinition : objectClassDefinition.getAttributeDefinitions(
                        ObjectClassDefinition.ALL)) {
                    Object value = configuration.getProperties() != null ? configuration.getProperties().get(
                            attributeDefinition.getID()) : null;
                    result.add(new ExtensionProperty(attributeDefinition, value));
                }
                extensionPropertyGroups.add(new ExtensionPropertyGroup(objectClassDefinition.getID(),
                                                                       objectClassDefinition.getName(),
                                                                       result));
            } catch (IOException ioException) {
                logger.warn("Failed to read configuration for {}", pid, ioException);
            }
        }
        return extensionPropertyGroups;
    }

    private void setConfiguration(Bundle bundle, String passedConfiguration) {
        Map<String, Map<String, Object>> initialConfiguration = new HashMap<>();
        if (passedConfiguration != null) {
            try {
                Map<String, Object> passedConfigurationMap = new ObjectMapper().readValue(passedConfiguration,
                                                                                          HashMap.class);
                initialConfiguration.put("*", passedConfigurationMap);
            } catch (IOException ioException) {
                logger.warn("Parsing configuration {} failed", passedConfiguration, ioException);
            }
        }
        mergeConfiguration(bundle, initialConfiguration);
    }
}
