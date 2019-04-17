/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

/**
 * @author Marc Gathier
 */
public abstract class AbstractArithmeticExpression implements Expression {
    private final String alias;
    private final Expression first;
    private final Expression second;


    public AbstractArithmeticExpression(String alias, Expression[] parameters) {
        this.alias = alias;
        this.first = parameters[0];
        this.second = parameters[1];
    }

    @Override
    public ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input) {
        ExpressionResult firstResult = first.apply(expressionContext, input);
        if( firstResult == null) {
            throw new IllegalArgumentException("Cannot evaluate " + alias);
        }
        ExpressionResult secondResult = second.apply(expressionContext, input);

        return doCompute(firstResult,secondResult);
    }

    protected abstract ExpressionResult doCompute(ExpressionResult first, ExpressionResult second);

    @Override
    public String alias() {
        return alias;
    }

}
