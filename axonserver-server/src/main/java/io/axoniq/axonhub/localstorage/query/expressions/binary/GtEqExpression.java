package io.axoniq.axonhub.localstorage.query.expressions.binary;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

import java.util.Objects;

/**
 * Author: marc
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
