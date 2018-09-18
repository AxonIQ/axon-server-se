package io.axoniq.axonserver.localstorage.query.result;

import io.axoniq.axonserver.localstorage.query.ExpressionResult;

/**
 * Author: marc
 */
public interface AbstractMapExpressionResult extends ExpressionResult {
    Iterable<String> getColumnNames();
}
