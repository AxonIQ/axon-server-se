package io.axoniq.axonserver.localstorage.query;

import io.axoniq.axondb.query.QueryElement;
import io.axoniq.axonserver.KeepNames;

import java.util.Optional;

@KeepNames
public interface ExpressionFactory {

    Optional<Expression> buildExpression(QueryElement element, ExpressionRegistry registry);

    Optional<PipeExpression> buildPipeExpression(QueryElement element, ExpressionRegistry registry);

}
