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
import io.axoniq.axonserver.localstorage.query.result.MapExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.StringExpressionResult;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonMap;

/**
 * @author Marc Gathier
 */
public class SubstringExpression implements Expression, PipeExpression {

    private final String alias;
    private final Expression[] expressions;

    public SubstringExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.expressions = expressions;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionResult value = expressions[0].apply(context, input);
        return (value != null && value.isNonNull()) ?
                new StringExpressionResult(doSubstring(context, input, value.toString())) :
                NullExpressionResult.INSTANCE ;
    }

    private String doSubstring(ExpressionContext context, ExpressionResult input, String value) {
        int from = expressions[1].apply(context, input).getNumericValue().intValue();
        if( expressions.length == 2) {
            return value.substring(from);
        }
        int to = expressions[2].apply(context, input).getNumericValue().intValue();
        if( from >= value.length()) return "";
        if( to >= value.length()) return value.substring(from);
        return value.substring(from, to);
    }

    @Override
    public String alias() {
        return alias;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        ExpressionResult apply = apply(context, result.getValue());
        return next.process(result.withValue(new MapExpressionResult(singletonMap(alias(), apply))));
    }

    @Override
    public List<String> getColumnNames(List<String> inputColumns) {
        return Collections.singletonList(alias);
    }
}
