package io.axoniq.axonhub.localstorage.query.expressions.binary;

import io.axoniq.axondb.query.QueryElement;
import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionRegistry;
import io.axoniq.axonhub.localstorage.query.PipeExpression;
import io.axoniq.axonhub.localstorage.query.expressions.AbstractExpressionFactory;

import java.util.Optional;

/**
 * Author: marc
 */
public class ArithmeticExpressionFactory extends AbstractExpressionFactory {
    public Optional<AbstractArithmeticExpression> doBuild(QueryElement element, ExpressionRegistry registry) {
        switch (element.operator()) {
            case "+":
                return Optional.of(new AddExpression(element.alias().orElse("plus"),
                        buildParameters(element.getParameters(), registry)));
            case "-":
                return Optional.of(new SubtractExpression(element.alias().orElse("subtract"),
                        buildParameters(element.getParameters(), registry)));
            case "*":
                return Optional.of(new MultiplyExpression(element.alias().orElse("multiply"),
                        buildParameters(element.getParameters(), registry)));
            case "/":
                return Optional.of(new DivideExpression(element.alias().orElse("divide"),
                        buildParameters(element.getParameters(), registry)));
            case "%":
                return Optional.of(new ModuloExpression(element.alias().orElse("modulo"),
                        buildParameters(element.getParameters(), registry)));
            default:
                
        }
        return Optional.empty();
    }

    @Override
    public Optional<Expression> buildExpression(QueryElement element, ExpressionRegistry registry) {
        return doBuild(element, registry).map(e -> (Expression)e);
    }

    @Override
    public Optional<PipeExpression> buildPipeExpression(QueryElement element, ExpressionRegistry registry) {
        return Optional.empty();
    }


}
