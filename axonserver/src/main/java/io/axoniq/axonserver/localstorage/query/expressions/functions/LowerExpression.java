package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.StringExpressionResult;

/**
 * Author: marc
 */
public class LowerExpression implements Expression {

    private final String alias;
    private final Expression expression;

    public LowerExpression(String alias, Expression expression) {
        this.alias = alias;
        this.expression = expression;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionResult value = expression.apply(context, input);
        return (value == null || !value.isNonNull()) ? NullExpressionResult.INSTANCE :  new StringExpressionResult(value.toString().toLowerCase());
    }

    @Override
    public String alias() {
        return alias;
    }

}
