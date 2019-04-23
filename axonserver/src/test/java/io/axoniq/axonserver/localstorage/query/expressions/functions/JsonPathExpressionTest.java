/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.expressions.Identifier;
import io.axoniq.axonserver.localstorage.query.expressions.StringLiteral;
import io.axoniq.axonserver.localstorage.query.result.ListExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.MapExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NumericExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.StringExpressionResult;
import org.junit.*;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marc Gathier
 */
public class JsonPathExpressionTest {
    private JsonPathExpression testSubject;
    private ExpressionContext expressionContext;
    private Map<String, ExpressionResult> map= new HashMap<>();

    @Before
    public void setUp()  {

        expressionContext = new ExpressionContext();
        map.put("test", new StringExpressionResult("{\n" +
                "    \"book\": \n" +
                "    [\n" +
                "        {\n" +
                "            \"title\": \"Beginning JSON\",\n" +
                "            \"author\": \"Ben Smith\",\n" +
                "            \"price\": 49\n" +
                "        },\n" +
                " \n" +
                "        {\n" +
                "            \"title\": \"JSON at Work\",\n" +
                "            \"author\": \"Tom Marrs\",\n" +
                "            \"price\": 29.99\n" +
                "        },\n" +
                " \n" +
                "        {\n" +
                "            \"title\": \"Learn JSON in a DAY\",\n" +
                "            \"author\": \"Acodemy\",\n" +
                "            \"price\": 8.99\n" +
                "        },\n" +
                " \n" +
                "        {\n" +
                "            \"title\": \"JSON: Questions and Answers\",\n" +
                "            \"author\": \"George Duckett\",\n" +
                "            \"price\": 6.00\n" +
                "        }\n" +
                "    ],\n" +
                " \n" +
                "    \"price range\": \n" +
                "    {\n" +
                "        \"cheap\": 10.00,\n" +
                "        \"medium\": 20.00\n" +
                "    }\n" +
                "}"));
    }

    @Test
    public void apply() {
        testSubject = new JsonPathExpression("title", new Expression[]{
                new Identifier("test"),
                new StringLiteral("$.book[*].title")
        });
        ExpressionResult result = testSubject.apply(expressionContext, new MapExpressionResult(map));
        assertThat(result).isInstanceOf(ListExpressionResult.class);
        ListExpressionResult resultList = (ListExpressionResult) result;
        assertThat(resultList.getValue()).contains(new StringExpressionResult("JSON: Questions and Answers"));
    }

    @Test
    public void applySingleValue() {
        testSubject = new JsonPathExpression("title", new Expression[]{
                new Identifier("test"),
                new StringLiteral("$.book[0].title")
        });
        ExpressionResult result = testSubject.apply(expressionContext, new MapExpressionResult(map));
        assertThat(result).isInstanceOf(StringExpressionResult.class);
        StringExpressionResult resultString = (StringExpressionResult) result;
        assertThat(resultString.getValue()).isEqualTo("Beginning JSON");
    }

    @Test
    public void applyNumberValue() {
        testSubject = new JsonPathExpression("title", new Expression[]{
                new Identifier("test"),
                new StringLiteral("$.book[0].price")
        });
        ExpressionResult result = testSubject.apply(expressionContext, new MapExpressionResult(map));
        assertThat(result).isInstanceOf(NumericExpressionResult.class);
        assertThat(result.getValue()).isEqualTo(BigDecimal.valueOf(49.0));
    }

    @Test
    public void applyNodes() {
        testSubject = new JsonPathExpression("title", new Expression[]{
                new Identifier("test"),
                new StringLiteral("$.book")
        });
        ExpressionResult result = testSubject.apply(expressionContext, new MapExpressionResult(map));
        assertThat(result).isInstanceOf(ListExpressionResult.class);
        ListExpressionResult resultList = (ListExpressionResult) result;
    }

}
