package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

/**
 * Author: marc
 */
public class DivideExpression extends AbstractArithmeticExpression {

    public DivideExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    @Override
    protected ExpressionResult doCompute(ExpressionResult first, ExpressionResult second) {
        return first.divide(second);
    }

}
