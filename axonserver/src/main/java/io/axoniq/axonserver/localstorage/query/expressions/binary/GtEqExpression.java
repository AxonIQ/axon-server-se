package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class GtEqExpression extends AbstractBooleanExpression {

    public GtEqExpression(String alias, Expression[] params) {
        super(alias, params);
    }

    @Override
    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        if (first == null && second != null) {
            return true;
        } else if (first != null && second == null) {
            return false;
        }
        return Objects.compare(first, second, ExpressionResult::compareTo) >= 0;
    }

}
