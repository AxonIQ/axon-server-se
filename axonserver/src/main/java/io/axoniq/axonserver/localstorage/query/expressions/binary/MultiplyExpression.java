package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

/**
 * Author: marc
 */
public class MultiplyExpression extends AbstractArithmeticExpression {

    public MultiplyExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    @Override
    protected ExpressionResult doCompute(ExpressionResult first, ExpressionResult second) {
        return first.multiply(second);
    }

}
