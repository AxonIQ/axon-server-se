/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.result;

import com.fasterxml.jackson.annotation.JsonValue;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class NumericExpressionResult implements ExpressionResult {
    private final BigDecimal value;

    public NumericExpressionResult(double value) {
        this(BigDecimal.valueOf(value));
    }

    public NumericExpressionResult(long value) {
        this(new BigDecimal(value));
    }

    public NumericExpressionResult(String value) {
        this(new BigDecimal(value));
    }

    public NumericExpressionResult(BigDecimal value) {
        this.value = value;
    }

    @JsonValue
    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public boolean isNonNull() {
        return value != null;
    }

    @Override
    public boolean isNumeric() {
        return true;
    }

    @Override
    public int compareTo(@NotNull ExpressionResult o) {
        if( value == null) return o.isNonNull() ? -1 : 0;

        if( o instanceof NumericExpressionResult) {
            return Double.compare(value.doubleValue(), ((NumericExpressionResult)o).value.doubleValue());
        }
        if( o instanceof StringExpressionResult) {
            return String.valueOf(value).compareTo(String.valueOf(o.getValue()));
        }

        return 0;
    }

    @Override
    public ExpressionResult divide(ExpressionResult other) {
        if( other instanceof NumericExpressionResult) {
            return new NumericExpressionResult(value.divide(((NumericExpressionResult) other).value, RoundingMode.HALF_UP));
        }
        throw new IllegalArgumentException("Invalid second parameter for divide: " + other.getClass().getSimpleName());
    }

    @Override
    public ExpressionResult add(ExpressionResult other) {
        if( other instanceof NumericExpressionResult) {
            return new NumericExpressionResult(value.add(((NumericExpressionResult) other).value));
        }
        throw new IllegalArgumentException("Invalid second parameter for add: " + other.getClass().getSimpleName());
    }

    @Override
    public ExpressionResult subtract(ExpressionResult other) {
        if( other instanceof NumericExpressionResult) {
            return new NumericExpressionResult(value.subtract(((NumericExpressionResult) other).value));
        }
        throw new IllegalArgumentException("Invalid second parameter for subtract: " + other.getClass().getSimpleName());
    }

    @Override
    public ExpressionResult multiply(ExpressionResult other) {
        if( other instanceof NumericExpressionResult) {
            return new NumericExpressionResult(value.multiply(((NumericExpressionResult) other).value));
        }
        throw new IllegalArgumentException("Invalid second parameter for multiply: " + other.getClass().getSimpleName());
    }
    @Override
    public ExpressionResult modulo(ExpressionResult other) {
        if( other instanceof NumericExpressionResult) {
            return new NumericExpressionResult(value.longValue() % other.getNumericValue().longValue());
        }
        throw new IllegalArgumentException("Invalid second parameter for modulo: " + other.getClass().getSimpleName());
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExpressionResult)) return false;
        ExpressionResult that = (ExpressionResult) o;
        return Objects.equals(value, that.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
