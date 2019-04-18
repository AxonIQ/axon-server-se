/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

/**
 * @author Marc Gathier
 */
public class Identifier implements Expression {

    private final String[] identifiers;
    private final String alias;

    public Identifier(String identifierName) {
        this( identifierName, identifierName);
    }

    public Identifier(String alias, String identifierName) {
        this.identifiers = identifierName.split("\\.");
        this.alias = alias;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult data) {
        if( data == null) return null;
        ExpressionResult result = data;
        for (String identifier : identifiers) {
            result = result.getByIdentifier(identifier);
        }
        return result;
    }

    @Override
    public String alias() {
        return alias;
    }
}
