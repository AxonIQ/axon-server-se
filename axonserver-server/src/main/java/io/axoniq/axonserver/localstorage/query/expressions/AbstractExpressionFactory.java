package io.axoniq.axonserver.localstorage.query.expressions;

import io.axoniq.axondb.query.QueryElement;
import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionFactory;
import io.axoniq.axonserver.localstorage.query.ExpressionRegistry;
import io.axoniq.axonserver.localstorage.query.QueryExecutionException;

import java.util.List;
import java.util.Optional;

public abstract class AbstractExpressionFactory implements ExpressionFactory {

    protected Expression[] buildParameters(List<? extends QueryElement> parameters, ExpressionRegistry registry) {
        Expression[] params = new Expression[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            params[i] = registry.resolveExpression(parameters.get(i));
        }
        return params;
    }

    protected Optional<Expression> optionalParameter(int index, List<? extends QueryElement> parameters, ExpressionRegistry registry) {
        if (parameters.size() > index) {
            return Optional.of(registry.resolveExpression(parameters.get(index)));
        }
        return Optional.empty();
    }

    protected Expression parameter(String functionName, int index,  List<? extends QueryElement> parameters, ExpressionRegistry registry) {
        return optionalParameter(index, parameters, registry).orElseThrow(() -> new QueryExecutionException("Function " +  functionName + " is missing parameter at index " + index));
    }
}
