package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.NumericExpressionResult;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: marc
 */
public class CountExpression extends AbstractAggregationFunction {

    private final Expression expression;

    public CountExpression(String alias, Expression expression) {
        super(alias);
        this.expression = expression;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionContext scoped = context.scoped(this);
        AtomicLong counter = scoped.computeIfAbsent(alias, AtomicLong::new);
        ExpressionResult expressionResult = expression == null ? input : expression.apply(context, input);
        return new NumericExpressionResult(counter.addAndGet(expressionResult.count()));
    }

}
