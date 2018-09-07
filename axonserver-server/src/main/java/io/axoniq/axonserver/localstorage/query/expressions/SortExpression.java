package io.axoniq.axonserver.localstorage.query.expressions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.PipeExpression;
import io.axoniq.axonserver.localstorage.query.Pipeline;
import io.axoniq.axonserver.localstorage.query.QueryResult;
import io.axoniq.axonserver.localstorage.query.result.ListExpressionResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: marc
 */
public class SortExpression implements PipeExpression {

    private final Expression[] sortKeys;

    public SortExpression(Expression[] sortKeys) {
        this.sortKeys = sortKeys;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        List<ExpressionResult> sortValues = new ArrayList<>();
        for (Expression key : sortKeys) {
            sortValues.add(key.apply(context, result.getValue()));
        }
        return next.process(result.withSortValues(new ListExpressionResult(sortValues)));
    }
}
