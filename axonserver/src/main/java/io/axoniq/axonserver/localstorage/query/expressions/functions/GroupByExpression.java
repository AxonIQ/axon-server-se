/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.*;
import io.axoniq.axonserver.localstorage.query.expressions.ListExpression;
import io.axoniq.axonserver.localstorage.query.result.ListExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.MapExpressionResult;

import java.util.*;

/**
 * @author Marc Gathier
 */
public class GroupByExpression implements PipeExpression {
    private final ListExpression grouper;
    private final Expression[] valueExpressions;

    public GroupByExpression(Expression[] expressions) {
        this.grouper = ListExpression.asListExpression(expressions[0]);
        this.valueExpressions = Arrays.copyOfRange(expressions, 1, expressions.length);
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        ListExpressionResult groupKey = grouper.apply(context, result.getValue());
        Map<String, ExpressionResult> values = new HashMap<>();
        for (int i = 0; i < grouper.items().size(); i++) {
            values.put(grouper.items().get(i).alias(), groupKey.getValue().get(i));
        }
        for (Expression valueExpression : valueExpressions) {
            values.put(valueExpression.alias(), valueExpression.apply(context.scoped(groupKey.getValue()), result.getValue()));
        }
        return next.process(result.withValue(new MapExpressionResult(values)).withId(groupKey));
    }

    @Override
    public List<String> getColumnNames(List<String> inputColumns) {
        List<String> names = new ArrayList<>();
        names.addAll(grouper.columnNames());
        for (Expression value : valueExpressions) {
            names.add(value.alias());
        }
        return names;
    }

}
