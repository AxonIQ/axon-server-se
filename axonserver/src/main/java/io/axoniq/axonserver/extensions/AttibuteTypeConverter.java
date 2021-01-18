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
public class AttibuteTypeConverter {

    public static AttributeType convert(int type) {
        switch (type) {
            case AttributeDefinition
                    .BOOLEAN:
                return AttributeType.BOOLEAN;
            case AttributeDefinition.STRING:
                return AttributeType.STRING;

            case AttributeDefinition.BIGINTEGER:
            case AttributeDefinition.LONG:
                return AttributeType.LONG;
            case AttributeDefinition.INTEGER:
                return AttributeType.INTEGER;
            case AttributeDefinition.SHORT:
                return AttributeType.INTEGER;
            case AttributeDefinition.CHARACTER:
                return AttributeType.STRING;
            case AttributeDefinition.BYTE:
                return AttributeType.INTEGER;
            case AttributeDefinition.BIGDECIMAL:
            case AttributeDefinition.DOUBLE:
                return AttributeType.DOUBLE;
            case AttributeDefinition.FLOAT:
                return AttributeType.FLOAT;
            case AttributeDefinition.PASSWORD:
                return AttributeType.PASSWORD;
        }
        return null;
    }
}
