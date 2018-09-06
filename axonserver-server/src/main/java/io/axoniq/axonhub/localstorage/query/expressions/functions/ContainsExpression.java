package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.expressions.binary.AbstractBooleanExpression;
import io.axoniq.axonhub.localstorage.query.result.ListExpressionResult;

/**
 * Author: marc
 */
public class ContainsExpression extends AbstractBooleanExpression {

    public ContainsExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    @Override
    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        String toSearch = String.valueOf(second.getValue());
        if (first instanceof ListExpressionResult) {
            return ((ListExpressionResult) first).getValue().stream()
                                                 .anyMatch(er -> toSearch.equals(String.valueOf(er.getValue())));
        }
        return String.valueOf(first.getValue()).contains(toSearch);
    }
}
