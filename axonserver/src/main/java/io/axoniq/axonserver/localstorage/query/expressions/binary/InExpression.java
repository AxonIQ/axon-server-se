package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.expressions.ListExpression;
import io.axoniq.axonserver.localstorage.query.result.BooleanExpressionResult;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class InExpression extends AbstractBooleanExpression {

    public InExpression(String alias, Expression[] params) {
        super(alias, params);
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult data) {
        ExpressionResult val1 = first.apply(context, data);
        if( second instanceof ListExpression) {
            for( Expression expression : ((ListExpression) second).items()) {
                ExpressionResult val2 = expression.apply(context, data);
                if( doEvaluate(val1, val2)) return BooleanExpressionResult.forValue(true);
            }
            return BooleanExpressionResult.forValue(false);
        }
        ExpressionResult val2 = second.apply(context, data);
        return BooleanExpressionResult.forValue(doEvaluate(val1, val2));
    }

    @Override
    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        return Objects.equals(first, second);
    }

}
