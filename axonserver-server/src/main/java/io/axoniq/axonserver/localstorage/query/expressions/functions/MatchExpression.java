package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.PipeExpression;
import io.axoniq.axonserver.localstorage.query.Pipeline;
import io.axoniq.axonserver.localstorage.query.QueryResult;
import io.axoniq.axonserver.localstorage.query.result.BooleanExpressionResult;

import java.util.List;

/**
 * Author: marc
 * match( value, pattern) or "value match pattern"
 */
public class MatchExpression implements Expression, PipeExpression {

    private final String alias;
    private final Expression valueExpression;
    private final Expression patternExpression;

    public MatchExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.valueExpression = expressions[0];
        this.patternExpression = expressions[1];
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionResult value = valueExpression.apply(context, input);
        if( value == null || ! value.isNonNull() ) return BooleanExpressionResult.forValue(false);

        ExpressionResult pattern = patternExpression.apply(context, input);
        return BooleanExpressionResult.forValue(value.toString().matches(pattern.toString()));
    }

    @Override
    public String alias() {
        return alias;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        if (apply(context, result.getValue()).isTrue()) {
            return next.process(result);
        }
        return true;
    }

    @Override
    public List<String> getColumnNames(List<String> inputColumns) {
        return inputColumns;
    }
}
