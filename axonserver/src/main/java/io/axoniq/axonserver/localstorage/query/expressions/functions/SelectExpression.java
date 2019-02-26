package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.*;
import io.axoniq.axonserver.localstorage.query.result.MapExpressionResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marc Gathier
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
