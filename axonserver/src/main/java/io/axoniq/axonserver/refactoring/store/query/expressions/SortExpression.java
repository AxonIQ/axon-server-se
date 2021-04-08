/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.store.query.expressions;

import io.axoniq.axonserver.refactoring.store.query.Expression;
import io.axoniq.axonserver.refactoring.store.query.ExpressionContext;
import io.axoniq.axonserver.refactoring.store.query.ExpressionResult;
import io.axoniq.axonserver.refactoring.store.query.PipeExpression;
import io.axoniq.axonserver.refactoring.store.query.Pipeline;
import io.axoniq.axonserver.refactoring.store.query.QueryResult;
import io.axoniq.axonserver.refactoring.store.query.result.ListExpressionResult;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marc Gathier
 */
public class SortExpression implements PipeExpression {

    private final Expression[] sortKeys;

    public SortExpression(Expression[] sortKeys) {
        this.sortKeys = sortKeys;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        List<ExpressionResult> sortValues = new ArrayList<>();
        for (Expression key : sortKeys) {
            sortValues.add(key.apply(context, result.getValue()));
        }
        return next.process(result.withSortValues(new ListExpressionResult(sortValues)));
    }
}
