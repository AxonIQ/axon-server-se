package io.axoniq.axonserver.localstorage.query;

import java.util.List;

public interface PipeExpression {

    boolean process(ExpressionContext context, QueryResult value, Pipeline next);

    default List<String> getColumnNames(List<String> inputColumns) {
        return inputColumns;
    }


}
