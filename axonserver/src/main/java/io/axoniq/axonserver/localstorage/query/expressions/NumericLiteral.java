package io.axoniq.axonserver.localstorage.query.expressions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NumericExpressionResult;

/**
 * @author Marc Gathier
 */
public class NumericLiteral implements Expression {
    private final String alias;
    private final String literal;

    public NumericLiteral(String alias, String literal) {
        this.alias = alias;
        this.literal = literal;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult data) {
        return new NumericExpressionResult(literal);
    }

    @Override
    public String alias() {
        return alias;
    }
}
