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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class ListExpressionResult implements ExpressionResult {
    private final List<ExpressionResult> results;

    public ListExpressionResult(ExpressionResult... results) {
        this(Arrays.asList(results));
    }

    public ListExpressionResult(List<ExpressionResult> results) {
        this.results = results;
    }

    /**
     * Returns a list containing the given {@code instance}, or {@code null} if the {@code instance} was {@code null}.
     *
     * @param instance The expressionResult to wrap in a list
     * @return An instance of a ListExpressionResult or null
     */
    public static ListExpressionResult asListOrNull(ExpressionResult instance) {
        if (instance == null) {
            return null;
        }
        return new ListExpressionResult(instance);
    }

    @JsonValue
    @Override
    public List<ExpressionResult> getValue() {
        return results;
    }

    @Override
    public boolean isNull() {
        return results == null;
    }

    @Override
    public boolean isNonNull() {
        return results != null;
    }

    @Override
    public int compareTo(ExpressionResult o) {
        if (o instanceof ListExpressionResult) {
            ListExpressionResult other = (ListExpressionResult) o;
            for (int i = 0; i < results.size(); i++) {
                if (other.count() <= i) {
                    return -1;
                }
                int compare = results.get(i).compareTo(other.results.get(i));
                if (compare != 0) {
                    return compare;
                }
            }
            if (other.count() > this.count()) {
                return 1;
            }
            return 0;
        }
        return -1;
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
