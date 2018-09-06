package io.axoniq.axonhub.localstorage.query.result;

import com.fasterxml.jackson.annotation.JsonValue;
import io.axoniq.axonhub.KeepNames;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Author: marc
 */
@KeepNames
public class ListExpressionResult implements ExpressionResult {
    private final List<ExpressionResult> results;

    public ListExpressionResult(ExpressionResult... results) {
        this(Arrays.asList(results));
    }

    public ListExpressionResult(List<ExpressionResult> results) {
        this.results = results;
    }

    @JsonValue
    @Override
    public List<ExpressionResult> getValue() {
        return results;
    }

    @Override
    public boolean isNonNull() {
        return results != null;
    }

    @Override
    public int compareTo(ExpressionResult o) {
        // TODO: Fix comparing of lists (first unequal item determines result)
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListExpressionResult that = (ListExpressionResult) o;
        return Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(results);
    }

    @Override
    public long count() {
        return isNonNull() ? results.stream().mapToLong(r -> r.isNonNull() ? 1 : 0).sum() : 0;
    }
}
