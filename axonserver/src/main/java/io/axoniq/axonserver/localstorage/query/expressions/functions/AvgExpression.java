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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Allard Buijze
 */
public class AvgExpression extends AbstractAggregationFunction{

    private final Expression expression;

    public AvgExpression(String alias, Expression expression) {
        super(alias);
        this.expression = expression;
    }

    @Override
    public ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input) {
        ExpressionContext scopedContext = expressionContext.scoped(this);
        ExpressionResult result = expression.apply(scopedContext, input);
        AtomicReference<AvgValue> initialSum = scopedContext.computeIfAbsent(alias, () -> new AtomicReference<>(new AvgValue()));
        if (result == null || !result.isNumeric() || result.getValue() == null) {
            return new NumericExpressionResult(initialSum.get().getAverage());
        }

        AvgValue newAvg = initialSum.accumulateAndGet(new AvgValue(1, result.getNumericValue()), AvgValue::add);

        return new NumericExpressionResult(newAvg.getAverage());
    }

    private static class AvgValue {

        private final long count;
        private final BigDecimal sum;

        public AvgValue() {
            this(0, BigDecimal.ZERO);
        }

        public AvgValue(long count, BigDecimal sum) {
            this.count = count;
            this.sum = sum;
        }

        public BigDecimal getAverage() {
            return count == 0 ? null : sum.divide(BigDecimal.valueOf(count), 8, RoundingMode.HALF_DOWN).stripTrailingZeros();
        }

        public AvgValue add(AvgValue other) {
            return new AvgValue(count + other.count, sum.add(other.sum));
        }
    }

}
