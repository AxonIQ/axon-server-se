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
import io.axoniq.axonserver.localstorage.query.result.NumericExpressionResult;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marc Gathier
 */
public class CountExpression extends AbstractAggregationFunction {

    private final Expression expression;

    public CountExpression(String alias, Expression expression) {
        super(alias);
        this.expression = expression;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionContext scoped = context.scoped(this);
        AtomicLong counter = scoped.computeIfAbsent(alias, AtomicLong::new);
        ExpressionResult expressionResult = expression == null ? input : expression.apply(context, input);
        if( expressionResult == null) {
            return new NumericExpressionResult(counter.get());
        }
        return new NumericExpressionResult(counter.addAndGet(expressionResult.count()));
    }

}
