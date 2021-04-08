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
import io.axoniq.axonserver.refactoring.store.query.PipeExpression;
import io.axoniq.axonserver.refactoring.store.query.Pipeline;
import io.axoniq.axonserver.refactoring.store.query.QueryResult;
import io.axoniq.axonserver.refactoring.store.query.result.MapExpressionResult;
import io.axoniq.axonserver.refactoring.store.query.result.StringExpressionResult;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonMap;

public abstract class AbstractAggregationFunction implements Expression, PipeExpression {

    protected final String alias;

    public AbstractAggregationFunction(String alias) {
        this.alias = alias;
    }

    @Override
    public String alias() {
        return alias;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        ExpressionResult apply = apply(context, result.getValue());
        return next.process(result.withValue(new MapExpressionResult(singletonMap(alias(), apply)))
                                  .withId(new StringExpressionResult(alias())));
    }

    @Override
    public List<String> getColumnNames(List<String> inputColumns) {
        return Collections.singletonList(alias);
    }
}
