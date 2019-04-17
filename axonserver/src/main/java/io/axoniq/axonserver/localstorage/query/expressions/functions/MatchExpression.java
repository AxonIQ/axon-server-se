/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.*;
import io.axoniq.axonserver.localstorage.query.result.BooleanExpressionResult;

import java.util.List;

/**
 * @author Marc Gathier
 * match( value, pattern) or "value match pattern"
 */
public class MatchExpression implements Expression, PipeExpression {

    private final String alias;
    private final Expression valueExpression;
    private final Expression patternExpression;

    public MatchExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.valueExpression = expressions[0];
        this.patternExpression = expressions[1];
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionResult value = valueExpression.apply(context, input);
        if( value == null || ! value.isNonNull() ) return BooleanExpressionResult.forValue(false);

        ExpressionResult pattern = patternExpression.apply(context, input);
        return BooleanExpressionResult.forValue(value.toString().matches(pattern.toString()));
    }

    @Override
    public String alias() {
        return alias;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        if (apply(context, result.getValue()).isTrue()) {
            return next.process(result);
        }
        return true;
    }

    @Override
    public List<String> getColumnNames(List<String> inputColumns) {
        return inputColumns;
    }
}
