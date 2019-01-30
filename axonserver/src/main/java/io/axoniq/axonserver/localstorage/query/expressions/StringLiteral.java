package io.axoniq.axonserver.localstorage.query.expressions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.StringExpressionResult;

/**
 * @author Marc Gathier
 */
public class StringLiteral implements Expression {
    private final String literal;

    public StringLiteral(String literal) {
        this.literal = literal;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult data) {
        return new StringExpressionResult(literal);
    }

    @Override
    public String alias() {
        return literal;
    }
}
