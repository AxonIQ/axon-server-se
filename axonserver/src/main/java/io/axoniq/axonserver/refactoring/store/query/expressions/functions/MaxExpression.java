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

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Marc Gathier
 */
public class MaxExpression extends AbstractAggregationFunction {

    private final Expression expression;

    public MaxExpression(String alias, Expression expression) {
        super(alias);
        this.expression = expression;
    }

    @Override
    public ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input) {
        ExpressionContext scopedContext = expressionContext.scoped(this);
        ExpressionResult result = expression.apply(scopedContext, input);
        return scopedContext.computeIfAbsent(alias, AtomicReference<ExpressionResult>::new)
                            .accumulateAndGet(result, (l, r) -> l == null ? r : l.max(r));
    }
}
