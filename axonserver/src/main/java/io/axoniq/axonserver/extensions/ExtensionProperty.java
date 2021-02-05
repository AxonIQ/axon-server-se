/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

/**
 * @author Marc Gathier
 */
public class ExtensionProperty {

    public static final String DUMMY_PASSWORD = "!DUMMY_PASSWORD!";

    private final String id;
    private final String name;
    private final Cardinality cardinality;
    private final Object defaultValue;
    private Object value;
    private final AttributeType type;
    private final String[] optionLabels;
    private final String[] optionValues;
    private final String description;

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

    public Cardinality getCardinality() {
        return cardinality;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public Object getValue() {
        if (AttributeType.PASSWORD.equals(type)) {
            return DUMMY_PASSWORD;
        }
        if (Cardinality.MULTI.equals(cardinality) && value == null) {
            return new Object[0];
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

    public Object internalValue() {
        return value;
    }
}
