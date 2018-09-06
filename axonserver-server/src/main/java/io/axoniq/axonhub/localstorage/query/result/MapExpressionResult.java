package io.axoniq.axonhub.localstorage.query.result;

import com.fasterxml.jackson.annotation.JsonValue;
import io.axoniq.axonhub.KeepNames;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Author: marc
 */
@KeepNames
public class MapExpressionResult implements AbstractMapExpressionResult {
    private final Map<String, ExpressionResult> result;

    public MapExpressionResult(Map<String, ExpressionResult> result) {
        this.result = new HashMap<>(result);
    }

    @JsonValue
    @Override
    public Object getValue() {
        return result;
    }

    @Override
    public int compareTo(ExpressionResult o) {
        return 0;
    }

    @Override
    public ExpressionResult getByIdentifier(String identifier) {
        return result.get(identifier);
    }

    @Override
    public boolean isNonNull() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapExpressionResult that = (MapExpressionResult) o;
        return Objects.equals(result, that.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(result);
    }

    @Override
    public Iterable<String> getColumnNames() {
        return result.keySet();
    }
}
