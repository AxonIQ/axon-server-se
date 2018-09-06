package io.axoniq.axonhub.localstorage.query.expressions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.PipeExpression;
import io.axoniq.axonhub.localstorage.query.Pipeline;
import io.axoniq.axonhub.localstorage.query.QueryResult;
import io.axoniq.axonhub.localstorage.query.result.ListExpressionResult;

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
