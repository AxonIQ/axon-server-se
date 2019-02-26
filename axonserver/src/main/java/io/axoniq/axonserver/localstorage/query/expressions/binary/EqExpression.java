package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class EqExpression extends AbstractBooleanExpression {

    public EqExpression(String alias, Expression[] params) {
        super(alias, params);
    }

    @Override
    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        return Objects.equals(first, second);
    }

}
