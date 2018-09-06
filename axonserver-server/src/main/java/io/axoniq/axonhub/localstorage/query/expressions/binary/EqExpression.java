package io.axoniq.axonhub.localstorage.query.expressions.binary;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

import java.util.Objects;

/**
 * Author: marc
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
