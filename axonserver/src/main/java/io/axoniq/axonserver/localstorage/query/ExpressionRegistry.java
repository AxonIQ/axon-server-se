package io.axoniq.axonserver.localstorage.query;

import io.axoniq.axondb.query.QueryElement;

import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Marc Gathier
 */
public class ExpressionRegistry {

    private List<ExpressionFactory> expressionFactories = new CopyOnWriteArrayList<>();

    public ExpressionRegistry() {
        this(ExpressionRegistry.class.getClassLoader());
    }

    public ExpressionRegistry(ClassLoader classLoader) {
        ServiceLoader.load(ExpressionFactory.class, classLoader).forEach(expressionFactories::add);
    }

    public Expression resolveExpression(QueryElement pipelineEntry) {
        for (ExpressionFactory expressionFactory : expressionFactories) {
            Optional<Expression> expression = expressionFactory.buildExpression(pipelineEntry, this);
            if (expression.isPresent()) {
                return expression.get();
            }
        }
        throw new IllegalArgumentException("No handler for expression: " + pipelineEntry.operator());
    }

    public PipeExpression resolvePipeExpression(QueryElement pipelineEntry) {
        for (ExpressionFactory expressionFactory : expressionFactories) {
            Optional<PipeExpression> expression = expressionFactory.buildPipeExpression(pipelineEntry, this);
            if (expression.isPresent()) {
                return expression.get();
            }
        }
        throw new IllegalArgumentException("No handler for top-level expression: " + pipelineEntry.operator());
    }

}
