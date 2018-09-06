package io.axoniq.axonhub.localstorage.query.expressions.binary;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

import java.util.Objects;

/**
 * Author: marc
 */
public class LtEqExpression extends AbstractBooleanExpression {

    public LtEqExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        return Objects.compare(first, second, ExpressionResult::compareTo) <= 0;
    }


}
