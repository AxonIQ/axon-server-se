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
import io.axoniq.axonserver.refactoring.store.query.result.ListExpressionResult;
import io.axoniq.axonserver.refactoring.store.query.result.NullExpressionResult;
import io.axoniq.axonserver.refactoring.store.query.result.StringExpressionResult;

import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class ConcatExpression implements Expression {

    private final String alias;
    private final Expression valueExpression;
    private final Expression charsExpression;

    public ConcatExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.valueExpression = expressions[0];
        this.charsExpression = expressions[1];
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ListExpressionResult value = (ListExpressionResult) valueExpression.apply(context, input);
        if (value == null || !value.isNonNull()) {
            return NullExpressionResult.INSTANCE;
        }

        String chars = charsExpression.apply(context, input).toString();

        String concatted = value.getValue().stream().map(String::valueOf).collect(Collectors.joining(chars));

        return new StringExpressionResult(concatted);
    }

    @Override
    public String alias() {
        return alias;
    }
}
