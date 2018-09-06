package io.axoniq.axonhub.localstorage.query.result;

import java.time.Instant;

/**
 * Author: marc
 */
public class TimestampExpressionResult extends NumericExpressionResult{

    public TimestampExpressionResult(long value) {
        super(value);
    }

    @Override
    public String toString() {
        return Instant.ofEpochMilli(getNumericValue().longValue()).toString();
    }
}
