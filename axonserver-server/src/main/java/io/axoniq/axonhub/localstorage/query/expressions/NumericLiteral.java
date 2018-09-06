package io.axoniq.axonhub.localstorage.query.expressions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.NumericExpressionResult;

/**
 * Author: marc
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
