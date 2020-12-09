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
    private final String[] defaultValue;
    private final Object value;
    private final int type;
    private final String[] optionLabels;
    private final String[] optionValues;
    private final String description;

    public ExtensionProperty(AttributeDefinition attributeDefinition, Object value) {
        this.id = attributeDefinition.getID();
        this.name = attributeDefinition.getName();
        this.cardinality = attributeDefinition.getCardinality();
        this.defaultValue = attributeDefinition.getDefaultValue();
        this.value = value;
        this.type = attributeDefinition.getType();
        this.optionLabels = attributeDefinition.getOptionLabels();
        this.optionValues = attributeDefinition.getOptionValues();
        this.description = attributeDefinition.getDescription();
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

    public String[] getDefaultValue() {
        return defaultValue;
    }

    public Object getValue() {
        return value;
    }

    public int getType() {
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
}
