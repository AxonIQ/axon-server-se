package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.PipeExpression;
import io.axoniq.axonhub.localstorage.query.Pipeline;
import io.axoniq.axonhub.localstorage.query.QueryResult;
import io.axoniq.axonhub.localstorage.query.result.MapExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.StringExpressionResult;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonMap;

/**
 * Author: marc
 */
public class SubstringExpression implements Expression, PipeExpression {

    private final String alias;
    private final Expression[] expressions;

    public SubstringExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.expressions = expressions;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionResult value = expressions[0].apply(context, input);
        return (value != null && value.isNonNull()) ?
                new StringExpressionResult(doSubstring(context, input, value.toString())) :
                NullExpressionResult.INSTANCE ;
    }

    private String doSubstring(ExpressionContext context, ExpressionResult input, String value) {
        int from = expressions[1].apply(context, input).getNumericValue().intValue();
        if( expressions.length == 2) {
            return value.substring(from);
        }
        int to = expressions[2].apply(context, input).getNumericValue().intValue();
        if( from >= value.length()) return "";
        if( to >= value.length()) return value.substring(from);
        return value.substring(from, to);
    }

    @Override
    public String alias() {
        return alias;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        ExpressionResult apply = apply(context, result.getValue());
        return next.process(result.withValue(new MapExpressionResult(singletonMap(alias(), apply))));
    }

    @Override
    public List<String> getColumnNames(List<String> inputColumns) {
        return Collections.singletonList(alias);
    }
}
