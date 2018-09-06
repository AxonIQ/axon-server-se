package io.axoniq.axonhub.localstorage.query.expressions.binary;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

/**
 * Author: marc
 */
public abstract class AbstractArithmeticExpression implements Expression {
    private final String alias;
    private final Expression first;
    private final Expression second;


    public AbstractArithmeticExpression(String alias, Expression[] parameters) {
        this.alias = alias;
        this.first = parameters[0];
        this.second = parameters[1];
    }

    @Override
    public ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input) {
        ExpressionResult firstResult = first.apply(expressionContext, input);
        if( firstResult == null) {
            throw new IllegalArgumentException("Cannot evaluate " + alias);
        }
        ExpressionResult secondResult = second.apply(expressionContext, input);

        return doCompute(firstResult,secondResult);
    }

    protected abstract ExpressionResult doCompute(ExpressionResult first, ExpressionResult second);

    @Override
    public String alias() {
        return alias;
    }

}
