package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NumericExpressionResult;

/**
 * @author Marc Gathier
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
