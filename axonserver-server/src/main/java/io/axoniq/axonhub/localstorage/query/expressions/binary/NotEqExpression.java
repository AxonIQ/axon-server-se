package io.axoniq.axonhub.localstorage.query.expressions.binary;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

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
