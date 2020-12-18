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
 * @author Marc Gathier
 */
@Component
public class ExtensionConfigurationManager {

    private final Logger logger = LoggerFactory.getLogger(ExtensionConfigurationManager.class);

    private final OsgiController osgiController;

    public ExtensionConfigurationManager(OsgiController osgiController) {
        this.osgiController = osgiController;
        osgiController.registerBundleListener(this::setConfiguration);
    }

    public void updateConfiguration(BundleInfo bundleInfo, Map<String, Object> properties) {
        Bundle bundle = osgiController.getBundle(bundleInfo);
        mergeConfiguration(bundle, properties);
    }

    private void mergeConfiguration(Bundle bundle, Map<String, Object> properties) {
        MetaTypeService metaTypeService = osgiController.getService(MetaTypeService.class);
        if (metaTypeService == null) {
            return;
        }

        ConfigurationAdmin configurationAdmin = osgiController.getService(ConfigurationAdmin.class);
        if (configurationAdmin == null) {
            return;
        }

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

                    updateWithCurrentConfiguration(configuration, updatedConfiguration);

                    updateWithProvidedProperties(properties, updatedConfiguration, validIds);
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

    public List<ExtensionProperty> configuration(BundleInfo bundleInfo) {
        Bundle bundle = osgiController.getBundle(bundleInfo);
        if (bundle == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Bundle not found");
        }

        MetaTypeService metaTypeService = osgiController.getService(MetaTypeService.class);
        if (metaTypeService == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "MetaTypeService not found");
        }

        ConfigurationAdmin configurationAdmin = osgiController.getService(ConfigurationAdmin.class);
        if (configurationAdmin == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "ConfigurationAdmin not found");
        }
        List<ExtensionProperty> result = new LinkedList<>();
        MetaTypeInformation info = metaTypeService.getMetaTypeInformation(bundle);
        for (String pid : info.getPids()) {
            try {
                Configuration configuration = configurationAdmin.getConfiguration(pid, bundle.getLocation());
                ObjectClassDefinition objectClassDefinition = info
                        .getObjectClassDefinition(pid, null);
                for (AttributeDefinition attributeDefinition : objectClassDefinition.getAttributeDefinitions(
                        ObjectClassDefinition.ALL)) {
                    Object value = configuration.getProperties() != null ? configuration.getProperties().get(
                            attributeDefinition.getID()) : null;
                    result.add(new ExtensionProperty(attributeDefinition, value));
                }
            } catch (IOException ioException) {
                logger.warn("Failed to read configuration for {}", pid, ioException);
            }
        }
        return result;
    }

    public void setConfiguration(Bundle bundle, String passedConfiguration) {
        Map<String, Object> passedConfigurationMap = new HashMap<>();
        if (passedConfiguration != null) {
            try {
                passedConfigurationMap = new ObjectMapper().readValue(passedConfiguration, HashMap.class);
            } catch (IOException ioException) {
                logger.warn("Parsing configuration {} failed", passedConfiguration, ioException);
            }
        }
        mergeConfiguration(bundle, passedConfigurationMap);
    }
}
