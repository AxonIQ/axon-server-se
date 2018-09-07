package io.axoniq.axonserver.localstorage.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.localstorage.query.result.ListExpressionResult;

import java.io.IOException;

public interface QueryResult {

    default String asJson(ObjectMapper objectMappper) throws IOException {
        return objectMappper.writeValueAsString(this);
    }

    ExpressionResult getValue();

    boolean isDeleted();

    ListExpressionResult getSortValues();

    ListExpressionResult getId();

    QueryResult withValue(ExpressionResult result);

    QueryResult deleted();

    QueryResult withSortValues(ListExpressionResult sortValues);

    QueryResult withId(ListExpressionResult identifyingValues);

    default QueryResult withId(ExpressionResult... identifyingValues) {
        return withId(new ListExpressionResult(identifyingValues));
    }
}
