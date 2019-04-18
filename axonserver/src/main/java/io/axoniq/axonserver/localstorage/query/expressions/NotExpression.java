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
import io.axoniq.axonserver.localstorage.query.PipeExpression;
import io.axoniq.axonserver.localstorage.query.Pipeline;
import io.axoniq.axonserver.localstorage.query.QueryResult;
import io.axoniq.axonserver.localstorage.query.result.BooleanExpressionResult;

/**
 * @author Marc Gathier
 */
public class NotExpression implements Expression, PipeExpression {

    private final String alias;
    private final Expression[] parameters;

    public NotExpression(String alias, Expression[] parameters) {
        this.alias = alias;
        this.parameters = parameters;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        Expression parameter = parameters[0];
        return parameter.apply(context, input).isTrue() ? BooleanExpressionResult.FALSE : BooleanExpressionResult.TRUE;
    }

    @Override
    public String alias() {
        return alias;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult value, Pipeline next) {
        if (apply(context, value.getValue()).isTrue()) {
            return next.process(value);
        }
        return true;
    }
}
