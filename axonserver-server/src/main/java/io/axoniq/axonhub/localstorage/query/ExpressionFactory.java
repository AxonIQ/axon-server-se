package io.axoniq.axonhub.localstorage.query;

import io.axoniq.axondb.query.QueryElement;
import io.axoniq.axonhub.KeepNames;

import java.util.Optional;

@KeepNames
public interface ExpressionFactory {

    Optional<Expression> buildExpression(QueryElement element, ExpressionRegistry registry);

    Optional<PipeExpression> buildPipeExpression(QueryElement element, ExpressionRegistry registry);

}
