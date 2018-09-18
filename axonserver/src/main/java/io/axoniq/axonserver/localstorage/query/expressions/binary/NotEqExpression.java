package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

import java.util.Objects;

/**
 * Author: marc
 */
public class NotEqExpression extends AbstractBooleanExpression {

    public NotEqExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    @Override
    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        return !Objects.equals(first, second);
    }
}
