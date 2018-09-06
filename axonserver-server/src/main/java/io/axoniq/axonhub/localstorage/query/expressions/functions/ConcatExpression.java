package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.ListExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.StringExpressionResult;

import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class ConcatExpression implements Expression {
    private final String alias;
    private final Expression valueExpression;
    private final Expression charsExpression;

    public ConcatExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.valueExpression = expressions[0];
        this.charsExpression = expressions[1];
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ListExpressionResult value = (ListExpressionResult)valueExpression.apply(context, input);
        if( value == null || ! value.isNonNull() ) return  NullExpressionResult.INSTANCE;

        String chars = charsExpression.apply(context, input).toString();

        String concatted = value.getValue().stream().map(String::valueOf).collect(Collectors.joining(chars));

        return new StringExpressionResult(concatted);
    }

    @Override
    public String alias() {
        return alias;
    }

}
