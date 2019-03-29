/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.expressions.Identifier;
import io.axoniq.axonserver.localstorage.query.expressions.StringLiteral;
import org.junit.*;

import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.*;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class FormatDateExpressionTest {
    private FormatDateExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new FormatDateExpression(null, new Identifier("value"),
                new Identifier("format"), new StringLiteral("UTC"));
        expressionContext = new ExpressionContext();
    }

    @Test
    public void apply() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", numericValue(1511442724079L),
                "format", stringValue("dd-MM-yyyy")));
        assertEquals("23-11-2017", actual.getValue());
    }

    @Test
    public void applyDay() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", numericValue(1511442724079L),
                "format", stringValue("F")));
        assertEquals("4", actual.getValue());
    }

    @Test
    public void applyWeek() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", numericValue(1511442724079L),
                "format", stringValue("w")));
        assertEquals("47", actual.getValue());
    }

    @Test
    public void applyMonth() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", numericValue(1511442724079L),
                "format", stringValue("MM")));
        assertEquals("11", actual.getValue());
    }

    @Test
    public void applyYear() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", numericValue(1511442724079L),
                "format", stringValue("yyyy")));
        assertEquals("2017", actual.getValue());
    }

    @Test
    public void applyHour() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", numericValue(1511442724079L),
                "format", stringValue("HH")));
        assertEquals("13", actual.getValue());
    }
}
