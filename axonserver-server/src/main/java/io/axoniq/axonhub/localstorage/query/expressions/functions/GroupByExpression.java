package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.PipeExpression;
import io.axoniq.axonhub.localstorage.query.Pipeline;
import io.axoniq.axonhub.localstorage.query.QueryResult;
import io.axoniq.axonhub.localstorage.query.expressions.ListExpression;
import io.axoniq.axonhub.localstorage.query.result.ListExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.MapExpressionResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: marc
 */
public class GroupByExpression implements PipeExpression {
    private final ListExpression grouper;
    private final Expression[] valueExpressions;

    public GroupByExpression(Expression[] expressions) {
        this.grouper = ListExpression.asListExpression(expressions[0]);
        this.valueExpressions = Arrays.copyOfRange(expressions, 1, expressions.length);
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        ListExpressionResult groupKey = grouper.apply(context, result.getValue());
        Map<String, ExpressionResult> values = new HashMap<>();
        for (int i = 0; i < grouper.items().size(); i++) {
            values.put(grouper.items().get(i).alias(), groupKey.getValue().get(i));
        }
        for (Expression valueExpression : valueExpressions) {
            values.put(valueExpression.alias(), valueExpression.apply(context.scoped(groupKey.getValue()), result.getValue()));
        }
        return next.process(result.withValue(new MapExpressionResult(values)).withId(groupKey));
    }

    @Override
    public List<String> getColumnNames(List<String> inputColumns) {
        List<String> names = new ArrayList<>();
        names.addAll(grouper.columnNames());
        for (Expression value : valueExpressions) {
            names.add(value.alias());
        }
        return names;
    }

}
