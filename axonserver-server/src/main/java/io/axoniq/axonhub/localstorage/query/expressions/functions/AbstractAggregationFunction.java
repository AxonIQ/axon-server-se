package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.PipeExpression;
import io.axoniq.axonhub.localstorage.query.Pipeline;
import io.axoniq.axonhub.localstorage.query.QueryResult;
import io.axoniq.axonhub.localstorage.query.result.MapExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.StringExpressionResult;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonMap;

public abstract class AbstractAggregationFunction implements Expression, PipeExpression {
    protected final String alias;

    public AbstractAggregationFunction(String alias) {
        this.alias = alias;
    }

    @Override
    public String alias() {
        return alias;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        ExpressionResult apply = apply(context, result.getValue());
        return next.process(result.withValue(new MapExpressionResult(singletonMap(alias(), apply)))
                                  .withId(new StringExpressionResult(alias())));
    }

    @Override
    public List<String> getColumnNames(List<String> inputColumns) {
        return Collections.singletonList(alias);
    }
}
