package io.axoniq.axonhub.localstorage.query.result;

import com.fasterxml.jackson.annotation.JsonValue;
import io.axoniq.axonhub.KeepNames;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * Author: marc
 */
@KeepNames
public class BooleanExpressionResult implements ExpressionResult {
    public static final BooleanExpressionResult TRUE = new BooleanExpressionResult(true);
    public static final BooleanExpressionResult FALSE = new BooleanExpressionResult(false);

    private final boolean booleanValue;

    private BooleanExpressionResult(boolean booleanValue) {
        this.booleanValue = booleanValue;
    }

    public static BooleanExpressionResult forValue(boolean value) {
        if (value) {
            return TRUE;
        }
        return FALSE;
    }

    @Override
    public boolean isTrue() {
        return booleanValue;
    }

    @JsonValue
    @Override
    public Object getValue() {
        return booleanValue;
    }

    @Override
    public boolean isNonNull() {
        return true;
    }

    @Override
    public long count() {
        return isTrue() ? 1 : 0;
    }

    @Override
    public int compareTo(@NotNull ExpressionResult o) {
        return Boolean.compare(booleanValue, o.isTrue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExpressionResult)) return false;
        ExpressionResult that = (ExpressionResult) o;
        return booleanValue == that.isTrue();
    }

    @Override
    public int hashCode() {
        return Objects.hash(booleanValue);
    }
}
