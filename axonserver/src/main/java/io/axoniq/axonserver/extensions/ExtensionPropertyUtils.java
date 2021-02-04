/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.rest.ExtensionPropertyGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.axoniq.axonserver.extensions.ExtensionProperty.DUMMY_PASSWORD;

/**
 * Utility methods to work with extension properties
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ExtensionPropertyUtils {

    private ExtensionPropertyUtils() {
    }

    /**
     * Checks types of properties provided and maps values to the correct type.
     * Throws an exception if one of properties cannot be mapped to the defined type.
     * @param properties map of properties per listener
     * @param listProperties defined properties per listener
     * @return updated map of properties per listener
     */
    public static Map<String, Map<String, Object>> validateProperties(Map<String, Map<String, Object>> properties,
                                                                      List<ExtensionPropertyGroup> listProperties) {
        Map<String, Map<String, Object>> validatedProperties = new HashMap<>();

        listProperties.forEach(propertyGroup -> {
            Map<String, Object> validatedPropertiesForGroup = new HashMap<>();
            validatedProperties.put(propertyGroup.getId(), validatedPropertiesForGroup);
            Map<String, Object> values = properties.getOrDefault(propertyGroup.getId(), Collections.emptyMap());
            propertyGroup.getProperties().forEach(property -> {
                Object value = values.get(property.getId());
                if (value == null) {
                    if (property.getDefaultValue() != null) {
                        validatedPropertiesForGroup.put(property.getId(), property.getDefaultValue());
                    }
                } else {
                    validatedPropertiesForGroup.put(property.getId(), convertType(value, property));
                }
            });
        });
        return validatedProperties;
    }

    private static Object convertType(Object value, ExtensionProperty extensionProperty) {
        if (Cardinality.MULTI.equals(extensionProperty.getCardinality())) {
            if (value.getClass().isArray()) {
                Object[] valueArr = (Object[]) value;
                List<Object> converted = new ArrayList<>();
                for (int i = 0; i < valueArr.length; i++) {
                    converted.add(convertType(valueArr[i], extensionProperty));
                }
                return converted;
            }

            if (value instanceof List) {
                List<Object> valueList = (List) value;
                List<Object> converted = new ArrayList<>();
                for (int i = 0; i < valueList.size(); i++) {
                    converted.add(convertType(valueList.get(i), extensionProperty));
                }
                return converted;
            }

            if (value instanceof String) {
                Object[] valueArr = ((String) value).split(",");
                List<Object> converted = new ArrayList<>();
                if (valueArr.length > 1) {
                    for (int i = 0; i < valueArr.length; i++) {
                        converted.add(convertType(valueArr[i], extensionProperty));
                    }
                    return converted;
                }
            }
        }

        switch (extensionProperty.getType()) {
            case STRING:
                return String.valueOf(value);
            case INTEGER:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                if (value instanceof String) {
                    try {
                        return Integer.parseInt((String) value);
                    } catch (NumberFormatException nfe) {
                        throw new MessagingPlatformException(ErrorCode.OTHER,
                                                             extensionProperty.getId()
                                                                     + ": Cannot convert value to integer");
                    }
                }
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     extensionProperty.getId() + ": Cannot convert value to integer");
            case LONG:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                if (value instanceof String) {
                    try {
                        return Long.parseLong((String) value);
                    } catch (NumberFormatException nfe) {
                        throw new MessagingPlatformException(ErrorCode.OTHER,
                                                             extensionProperty.getId()
                                                                     + ": Cannot convert value to long");
                    }
                }
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     extensionProperty.getId() + ": Cannot convert value to long");
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                if (value instanceof String) {
                    try {
                        return Float.parseFloat((String) value);
                    } catch (NumberFormatException nfe) {
                        throw new MessagingPlatformException(ErrorCode.OTHER,
                                                             extensionProperty.getId()
                                                                     + ": Cannot convert value to float");
                    }
                }
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     extensionProperty.getId() + ": Cannot convert value to float");
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                if (value instanceof String) {
                    try {
                        return Double.parseDouble((String) value);
                    } catch (NumberFormatException nfe) {
                        throw new MessagingPlatformException(ErrorCode.OTHER,
                                                             extensionProperty.getId()
                                                                     + ": Cannot convert value to double");
                    }
                }
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     extensionProperty.getId() + ": Cannot convert value to double");
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                if (value instanceof String) {
                    try {
                        return Boolean.parseBoolean((String) value);
                    } catch (NumberFormatException nfe) {
                        throw new MessagingPlatformException(ErrorCode.OTHER,
                                                             extensionProperty.getId()
                                                                     + ": Cannot convert value to boolean");
                    }
                }
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     extensionProperty.getId() + ": Cannot convert value to boolean");
            case PASSWORD:
                if (DUMMY_PASSWORD.equals(value)) {
                    return extensionProperty.internalValue();
                }
                return String.valueOf(value);
        }
        throw new MessagingPlatformException(ErrorCode.OTHER, extensionProperty.getId() + ": Unknown type");
    }

    /**
     * Merges the set properties with the default values of the properties.
     *
     * @param definedProperties                properties defined for an extension
     * @param serializedConfiguration          serialized currently set values for the properties
     * @param extensionConfigurationSerializer serializer to deserialize the values
     */
    public static void setValues(List<ExtensionPropertyGroup> definedProperties,
                                 String serializedConfiguration,
                                 ExtensionConfigurationSerializer extensionConfigurationSerializer) {
        Map<String, Map<String, Object>> configuration = extensionConfigurationSerializer.deserialize(
                serializedConfiguration);
        definedProperties.forEach(propertyGroup -> {
            Map<String, Object> configurationForGroup = configuration == null ?
                    Collections.emptyMap() :
                    configuration.getOrDefault(propertyGroup.getId(), Collections.emptyMap());
            propertyGroup.getProperties().forEach(prop -> prop
                    .setValue(configurationForGroup
                                      .getOrDefault(prop.getId(), prop.getDefaultValue())));
        });
    }
}
