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
import io.axoniq.axonserver.localstorage.query.QueryResult;
import io.axoniq.axonserver.localstorage.query.expressions.Identifier;
import io.axoniq.axonserver.localstorage.query.expressions.NumericLiteral;
import io.axoniq.axonserver.localstorage.query.expressions.binary.AddExpression;
import io.axoniq.axonserver.localstorage.query.result.DefaultQueryResult;
import io.axoniq.axonserver.localstorage.query.result.MapExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NumericExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.StringExpressionResult;
import org.junit.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marc Gathier
 */
public class SelectExpressionTest {
    private SelectExpression testSubject;
    private ExpressionContext context;

    @Before
    public void setup() {

        AddExpression addExpression = new AddExpression("sum", new Expression[] {
                new NumericLiteral("a", "100"),
                new Identifier("test2")
        });
        Expression[] expressions = {
                new Identifier("test"),
                addExpression
        };
        testSubject = new SelectExpression(expressions);
        context = new ExpressionContext();

    }

    @Test
    public void process() {
        Map<String, ExpressionResult> map = new HashMap<>();
        map.put("test2", new NumericExpressionResult(100));
        map.put("test", new StringExpressionResult("String value"));
        QueryResult queryResult = new DefaultQueryResult(new MapExpressionResult(map));

        testSubject.process(context, queryResult, value -> {
            assertThat(value.getValue()).isInstanceOf(MapExpressionResult.class);
            MapExpressionResult map1 = (MapExpressionResult)value.getValue();
            assertThat(map1.getByIdentifier("sum")).isInstanceOf(NumericExpressionResult.class);
            assertThat(map1.getByIdentifier("test")).isInstanceOf(StringExpressionResult.class);
            return true;
        });
        assertThat(testSubject.getColumnNames(Collections.emptyList())).containsSequence("test", "sum");
    }
}
