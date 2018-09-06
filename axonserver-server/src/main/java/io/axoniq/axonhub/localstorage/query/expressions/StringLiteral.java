package io.axoniq.axonhub.localstorage.query.expressions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.StringExpressionResult;

/**
 * Author: marc
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
