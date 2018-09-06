package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.NumericExpressionResult;

/**
 * Author: marc
 */
public class LengthExpression implements Expression {

    private final String alias;
    private final Expression expression;

    public LengthExpression(String alias, Expression expression) {
        this.alias = alias;
        this.expression = expression;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        Object value = expression.apply(context, input).getValue();
        return value == null ? NullExpressionResult.INSTANCE :  new NumericExpressionResult(String.valueOf(value).length());
    }

    @Override
    public String alias() {
        return alias;
    }

}
