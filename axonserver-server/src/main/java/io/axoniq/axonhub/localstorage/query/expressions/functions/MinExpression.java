package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Author: marc
 */
public class MinExpression extends AbstractAggregationFunction {

    private final Expression expression;

    public MinExpression(String alias, Expression expression) {
        super(alias);
        this.expression = expression;
    }

    @Override
    public ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input) {
        ExpressionContext scopedContext = expressionContext.scoped(this);
        ExpressionResult result = expression.apply(scopedContext, input);
        return scopedContext.computeIfAbsent(alias, AtomicReference<ExpressionResult>::new)
                .accumulateAndGet(result, (l, r) -> l == null ? r : l.min(r));
    }
}
