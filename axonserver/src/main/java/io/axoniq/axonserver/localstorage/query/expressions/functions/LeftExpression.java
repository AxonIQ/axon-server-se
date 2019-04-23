/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.StringExpressionResult;

/**
 * @author Marc Gathier
 * left(value, chars)
 */
public class LeftExpression implements Expression {

    private final String alias;
    private final Expression valueExpression;
    private final Expression charsExpression;

    public LeftExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.valueExpression = expressions[0];
        this.charsExpression = expressions[1];
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionResult value = valueExpression.apply(context, input);
        if( value == null || ! value.isNonNull() ) return  NullExpressionResult.INSTANCE;

        int chars = charsExpression.apply(context, input).getNumericValue().intValue();

        String string = value.toString();
        if( string.length() < chars) return new StringExpressionResult(string);
        return new StringExpressionResult(string.substring(0, chars));
    }

    @Override
    public String alias() {
        return alias;
    }

}
