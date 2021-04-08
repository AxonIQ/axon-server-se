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

import java.util.Objects;

public class MetaDataExpression implements Expression {

    private final String alias;
    private final Expression[] expressions;

    public MetaDataExpression(String alias, Expression[] keyPath) {
        this.alias = alias == null ? buildAlias(keyPath) : alias;
        this.expressions = keyPath;
    }

    private String buildAlias(Expression[] keyPath) {
        StringBuilder sb = new StringBuilder("metaData");
        for (Expression expression : keyPath) {
            sb.append(".");
            sb.append(expression.alias());
        }
        return sb.toString();
    }

    @Override
    public ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input) {
        ExpressionResult result = input.getByIdentifier("metaData");
        for (Expression id : expressions) {
            String identifier = Objects.toString(id.apply(expressionContext, input).getValue());
            result = result.getByIdentifier(identifier);
        }
        return result;
    }

    @Override
    public String alias() {
        return alias;
    }
}
