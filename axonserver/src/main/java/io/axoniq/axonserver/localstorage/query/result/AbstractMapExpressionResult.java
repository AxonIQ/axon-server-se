package io.axoniq.axonserver.localstorage.query.result;

import io.axoniq.axonserver.localstorage.query.ExpressionResult;

/**
 * @author Marc Gathier
 */
public interface AbstractMapExpressionResult extends ExpressionResult {
    Iterable<String> getColumnNames();
}
