package io.axoniq.axonserver.localstorage.query.expressions.timeconstraints;

import io.axoniq.axondb.query.QueryElement;
import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionRegistry;
import io.axoniq.axonserver.localstorage.query.PipeExpression;
import io.axoniq.axonserver.localstorage.query.expressions.AbstractExpressionFactory;

import java.util.Optional;

/**
 * Author: marc
 */
public class TimeConstraintExpressionFactory extends AbstractExpressionFactory {
    @Override
    public Optional<Expression> buildExpression(QueryElement element, ExpressionRegistry registry) {
        return Optional.empty();
    }

    @Override
    public Optional<PipeExpression> buildPipeExpression(QueryElement element, ExpressionRegistry registry) {
        String s = element.operator().toLowerCase();// in case last is used as keyword: 'last 5 minutes"
        if ("k_last".equals(s) || "last".equals(s)) {
            // this is applied as an input level filter, so we don't need to filter in the pipeline
            return Optional.of(NoOpExpression.instance());
        }
        return Optional.empty();
    }
}
