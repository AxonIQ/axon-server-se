package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.StringExpressionResult;

/**
 * Author: marc
 */
public class UpperExpression implements Expression {

    private final String alias;
    private final Expression expression;

    public UpperExpression(String alias, Expression expression) {
        this.alias = alias;
        this.expression = expression;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionResult value = expression.apply(context, input);
        return (value == null || !value.isNonNull()) ? NullExpressionResult.INSTANCE :  new StringExpressionResult(value.toString().toUpperCase());
    }

    @Override
    public String alias() {
        return alias;
    }

}
