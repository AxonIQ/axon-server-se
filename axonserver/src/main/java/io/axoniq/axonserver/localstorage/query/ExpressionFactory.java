package io.axoniq.axonserver.localstorage.query;

import io.axoniq.axondb.query.QueryElement;

import java.util.Optional;

public interface ExpressionFactory {

    Optional<Expression> buildExpression(QueryElement element, ExpressionRegistry registry);

    Optional<PipeExpression> buildPipeExpression(QueryElement element, ExpressionRegistry registry);

}
