/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.functions;

import com.jayway.jsonpath.JsonPath;
import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.*;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marc Gathier
 */
public class JsonPathExpression implements Expression {
    private final String alias;
    private final Expression document;
    private final Expression jsonPath;

    public JsonPathExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.document = expressions[0];
        this.jsonPath = expressions[1];
    }

    @Override
    public ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input) {
        ExpressionResult doc = document.apply(expressionContext, input);
        String json = jsonPath.apply(expressionContext, input).getValue().toString();
        Object result = JsonPath.compile(json).read(doc.asJson());
        List<ExpressionResult> values = new ArrayList<>();
        boolean isList = false;
        if( result instanceof JSONArray) {
            isList = true;
            JSONArray resultArray = (JSONArray)result;
            resultArray.forEach(value ->  values.add(toExpressionResult(value)));
        } else {
            values.add(toExpressionResult(result));
        }
        if( ! isList) {
            if( values.size() == 1 ) return values.get(0);
            return NullExpressionResult.INSTANCE;
        }
        return new ListExpressionResult(values);
    }

    private ExpressionResult toExpressionResult(Object value) {
        if( value instanceof String) return new StringExpressionResult((String)value);
        if( value instanceof Number) return new NumericExpressionResult(((Number)value).doubleValue());
        if( value instanceof JSONObject) return new JSONExpressionResult(((JSONObject)value));
        return NullExpressionResult.INSTANCE;
    }

    @Override
    public String alias() {
        return alias;
    }
}
