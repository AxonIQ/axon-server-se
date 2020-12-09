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
import org.osgi.framework.Bundle;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.metatype.AttributeDefinition;
import org.osgi.service.metatype.MetaTypeInformation;
import org.osgi.service.metatype.MetaTypeService;
import org.osgi.service.metatype.ObjectClassDefinition;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Marc Gathier
 */
@Component
public class ExtensionConfigurationManager {

    private final OsgiController osgiController;

    public ExtensionConfigurationManager(OsgiController osgiController) {
        this.osgiController = osgiController;
        osgiController.registerBundleListener(this::setConfiguration);
    }

    public void updateConfiguration(BundleInfo bundleInfo, Map<String, String> properties) {
        Bundle bundle = osgiController.getBundle(bundleInfo);
        MetaTypeService metaTypeService = osgiController.getService(MetaTypeService.class);
        if (metaTypeService == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "MetaTypeService not found");
        }

        ConfigurationAdmin configurationAdmin = osgiController.getService(ConfigurationAdmin.class);
        if (configurationAdmin == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "ConfigurationAdmin not found");
        }
        MetaTypeInformation info = metaTypeService.getMetaTypeInformation(bundle);
        for (String pid : info.getPids()) {
            try {
                Configuration configuration = configurationAdmin.getConfiguration(pid, "?");
                Dictionary<String, Object> current = configuration.getProperties();
                if (current == null) {
                    current = new Hashtable<>();
                }
                properties.forEach(current::put);
                configuration.update(current);
            } catch (IOException ioException) {
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     "Error retrieving configuration for " + pid,
                                                     ioException);
            }
        }
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
                Configuration configuration = configurationAdmin.getConfiguration(pid);
                ObjectClassDefinition objectClassDefinition = info
                        .getObjectClassDefinition(pid, null);
                for (AttributeDefinition attributeDefinition : objectClassDefinition.getAttributeDefinitions(
                        ObjectClassDefinition.ALL)) {
                    Object value = configuration.getProperties() != null ? configuration.getProperties().get(
                            attributeDefinition.getID()) : null;
                    result.add(new ExtensionProperty(attributeDefinition, value));
                }
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        return result;
    }

    public void setConfiguration(Bundle bundle) {
        MetaTypeService metaTypeService = osgiController.getService(MetaTypeService.class);
        if (metaTypeService == null) {
            return;
        }

        ConfigurationAdmin configurationAdmin = osgiController.getService(ConfigurationAdmin.class);
        if (configurationAdmin == null) {
            return;
        }
        System.out.println(configurationAdmin);

        MetaTypeInformation info = metaTypeService.getMetaTypeInformation(bundle);
        for (String pid : info.getPids()) {
            try {
                Configuration configuration = configurationAdmin.getConfiguration(pid, "?");
                System.out.printf("%s/%s: %s old properties %s%n",
                                  bundle.getSymbolicName(),
                                  bundle.getVersion(),
                                  pid,
                                  configuration.getProperties());

                if (configuration.getProperties() == null || configuration.getProperties().isEmpty()) {
                    ObjectClassDefinition objectClassDefinition = info
                            .getObjectClassDefinition(pid, null);
                    Dictionary<String, Object> defaultValues = new Hashtable<>();
                    for (AttributeDefinition attributeDefinition : objectClassDefinition.getAttributeDefinitions(
                            ObjectClassDefinition.ALL)) {
                        if (attributeDefinition.getDefaultValue() != null &&
                                attributeDefinition.getDefaultValue().length > 0) {
                            defaultValues.put(attributeDefinition.getID(),
                                              attributeDefinition.getDefaultValue());
                        }
                    }
                    configuration.update(defaultValues);
                    System.out.printf("%s/%s: new properties %s%n", bundle.getSymbolicName(), bundle.getVersion(),
                                      configurationAdmin.getConfiguration(pid).getProperties());
                }
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
}
