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
import io.axoniq.axonserver.localstorage.query.result.ListExpressionResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class ListExpression implements Expression {
    private final String alias;
    private final Expression[] expressions;

    public ListExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.expressions = expressions;
    }

    public static ListExpression asListExpression(Expression expression) {
        if (expression instanceof ListExpression) {
            return (ListExpression) expression;
        } else {
            return new ListExpression(expression.alias(), new Expression[]{expression});
        }
    }

    @Override
    public ListExpressionResult apply(ExpressionContext context, ExpressionResult data) {
        List<ExpressionResult> results = new ArrayList<>(expressions.length);
        for (Expression item : expressions) {
            results.add(item.apply(context, data));
        }
        return new ListExpressionResult(results);
    }

    public List<Expression> items() {
        return Arrays.asList(expressions);
    }

    @Override
    public String alias() {
        return alias;
    }

    public List<String> columnNames() {
        return Arrays.stream(expressions).map(Expression::alias).collect(Collectors.toList());
    }
}
