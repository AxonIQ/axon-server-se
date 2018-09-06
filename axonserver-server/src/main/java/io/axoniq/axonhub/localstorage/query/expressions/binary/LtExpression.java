package io.axoniq.axonhub.localstorage.query.expressions.binary;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

import java.util.Objects;

/**
 * Author: marc
 */
public class LtExpression extends AbstractBooleanExpression {

    public LtExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        return Objects.compare(first, second, ExpressionResult::compareTo) < 0;
    }


}
