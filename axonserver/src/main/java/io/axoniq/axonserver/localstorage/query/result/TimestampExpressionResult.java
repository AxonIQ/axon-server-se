package io.axoniq.axonserver.localstorage.query.result;

import java.time.Instant;

/**
 * @author Marc Gathier
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
