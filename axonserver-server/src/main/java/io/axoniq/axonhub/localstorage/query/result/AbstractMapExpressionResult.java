package io.axoniq.axonhub.localstorage.query.result;

import io.axoniq.axonhub.localstorage.query.ExpressionResult;

/**
 * Author: marc
 */
public interface AbstractMapExpressionResult extends ExpressionResult {
    Iterable<String> getColumnNames();
}
