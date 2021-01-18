/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import org.osgi.service.metatype.AttributeDefinition;

/**
 * @author Marc Gathier
 */
public class ExtensionProperty {

    private final String id;
    private final String name;
    private final int cardinality;
    private final String defaultValue;
    private Object value;
    private final AttributeType type;
    private final String[] optionLabels;
    private final String[] optionValues;
    private final String description;

    public ExtensionProperty(AttributeDefinition attributeDefinition, Object value) {
        this.id = attributeDefinition.getID();
        this.name = attributeDefinition.getName();
        this.cardinality = attributeDefinition.getCardinality();
        this.defaultValue =
                attributeDefinition.getDefaultValue() != null && attributeDefinition.getDefaultValue().length > 0 ?
                        attributeDefinition.getDefaultValue()[0] : null;
        this.value = value;
        this.type = AttibuteTypeConverter.convert(attributeDefinition.getType());
        this.optionLabels = attributeDefinition.getOptionLabels();
        this.optionValues = attributeDefinition.getOptionValues();
        this.description = attributeDefinition.getDescription();
    }

    public ExtensionProperty(ExtensionPropertyDefinition extensionPropertyDefinition) {
        this.id = extensionPropertyDefinition.id();
        this.name = extensionPropertyDefinition.name();
        this.cardinality = extensionPropertyDefinition.cardinality();
        this.defaultValue = extensionPropertyDefinition.defaultValue();
        this.value = extensionPropertyDefinition.defaultValue();
        this.type = extensionPropertyDefinition.type();
        this.optionLabels = extensionPropertyDefinition.optionLabels() == null ?
                new String[0] :
                extensionPropertyDefinition.optionLabels().toArray(new String[0]);
        this.optionValues = extensionPropertyDefinition.optionValues() == null ?
                new String[0] :
                extensionPropertyDefinition.optionValues().toArray(new String[0]);
        this.description = extensionPropertyDefinition.description();
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getCardinality() {
        return cardinality;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Object getValue() {
        if (AttributeType.PASSWORD.equals(type)) {
            return value == null ? null : "********";
        }
        return value;
    }

    public AttributeType getType() {
        return type;
    }

    public String[] getOptionLabels() {
        return optionLabels;
    }

    public String[] getOptionValues() {
        return optionValues;
    }

    public String getDescription() {
        return description;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
