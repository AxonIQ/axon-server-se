/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.store.query.expressions.functions;

import io.axoniq.axonserver.refactoring.store.query.Expression;
import io.axoniq.axonserver.refactoring.store.query.ExpressionContext;
import io.axoniq.axonserver.refactoring.store.query.ExpressionResult;
import io.axoniq.axonserver.refactoring.store.query.result.NullExpressionResult;
import io.axoniq.axonserver.refactoring.store.query.result.NumericExpressionResult;

/**
 * @author Marc Gathier
 */
public class LengthExpression implements Expression {

    private final String alias;
    private final Expression expression;

    public LengthExpression(String alias, Expression expression) {
        this.alias = alias;
        this.expression = expression;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        Object value = expression.apply(context, input).getValue();
        return value == null ? NullExpressionResult.INSTANCE : new NumericExpressionResult(String.valueOf(value)
                                                                                                 .length());
    }

    @Override
    public String alias() {
        return alias;
    }
}
