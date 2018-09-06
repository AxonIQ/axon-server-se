package io.axoniq.axonhub.localstorage.query.expressions.binary;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

import java.util.Objects;

/**
 * Author: marc
 */
public class GtExpression extends AbstractBooleanExpression {

    public GtExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    @Override
    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        if (first == null && second != null) {
            return true;
        } else if (first != null && second == null) {
            return false;
        }
        return Objects.compare(first, second, ExpressionResult::compareTo) > 0;
    }
}
