package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.PipeExpression;
import io.axoniq.axonhub.localstorage.query.Pipeline;
import io.axoniq.axonhub.localstorage.query.QueryResult;
import io.axoniq.axonhub.localstorage.query.result.MapExpressionResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: marc
 */
public class SelectExpression implements PipeExpression {
    private final Expression[] expressions;

    public SelectExpression(Expression[] expressions) {
        this.expressions = expressions;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        Map<String, ExpressionResult> values = new HashMap<>();
        for (Expression valueExpression : expressions) {
            values.put(valueExpression.alias(), valueExpression.apply(context, result.getValue()));
        }
        return next.process(result.withValue(new MapExpressionResult(values)));
    }

    @Override
    public List<String> getColumnNames(List<String> inputColumns) {
        List<String> names = new ArrayList<>();
        for (Expression value : expressions) {
            names.add(value.alias());
        }
        return names;
    }

}
