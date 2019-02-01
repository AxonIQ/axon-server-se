package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

/**
 * @author Marc Gathier
 */
public class SubtractExpression extends AbstractArithmeticExpression {

    public SubtractExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    @Override
    protected ExpressionResult doCompute(ExpressionResult first, ExpressionResult second) {
        return first.subtract(second);
    }

}
