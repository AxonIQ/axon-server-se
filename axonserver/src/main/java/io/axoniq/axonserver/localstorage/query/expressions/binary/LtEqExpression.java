package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class LtEqExpression extends AbstractBooleanExpression {

    public LtEqExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        return Objects.compare(first, second, ExpressionResult::compareTo) <= 0;
    }


}
