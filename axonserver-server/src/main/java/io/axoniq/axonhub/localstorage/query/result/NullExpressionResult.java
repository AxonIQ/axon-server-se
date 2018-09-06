package io.axoniq.axonhub.localstorage.query.result;

import com.fasterxml.jackson.annotation.JsonValue;
import io.axoniq.axonhub.KeepNames;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

import java.util.Objects;

/**
 * Author: marc
 */
@KeepNames
public class NullExpressionResult implements ExpressionResult {
    public static final NullExpressionResult INSTANCE = new NullExpressionResult();
    private NullExpressionResult() {

    }

    @JsonValue
    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public boolean isNonNull() {
        return false;
    }

    @Override
    public boolean isNumeric() {
        return false;
    }

    @Override
    public int compareTo(ExpressionResult o) {
        if( o.isNonNull()) return -1;
        return 0;
    }

    @Override
    public String toString() {
        return "<Null>";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExpressionResult)) return false;
        ExpressionResult that = (ExpressionResult) o;
        return !that.isNonNull();
    }

    @Override
    public int hashCode() {
        return Objects.hash((Object) null);
    }

}
