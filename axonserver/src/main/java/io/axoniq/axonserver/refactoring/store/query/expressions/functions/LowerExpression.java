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
import io.axoniq.axonserver.refactoring.store.query.result.StringExpressionResult;

/**
 * @author Marc Gathier
 */
public class LowerExpression implements Expression {

    private final String alias;
    private final Expression expression;

    public LowerExpression(String alias, Expression expression) {
        this.alias = alias;
        this.expression = expression;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionResult value = expression.apply(context, input);
        return (value == null || !value
                .isNonNull()) ? NullExpressionResult.INSTANCE : new StringExpressionResult(value.toString()
                                                                                                .toLowerCase());
    }

    @Override
    public String alias() {
        return alias;
    }
}
