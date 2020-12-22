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

    public static String convert(int type) {
        switch (type) {
            case AttributeDefinition
                    .BOOLEAN:
                return "boolean";
            case AttributeDefinition.STRING:
                return "string";

            case AttributeDefinition.BIGINTEGER:
            case AttributeDefinition.LONG:
                return "long";
            case AttributeDefinition.INTEGER:
                return "integer";
            case AttributeDefinition.SHORT:
                return "short";
            case AttributeDefinition.CHARACTER:
                return "character";
            case AttributeDefinition.BYTE:
                return "byte";
            case AttributeDefinition.BIGDECIMAL:
            case AttributeDefinition.DOUBLE:
                return "double";
            case AttributeDefinition.FLOAT:
                return "float";
            case AttributeDefinition.PASSWORD:
                return "password";
        }
        return "unknown";
    }
}
