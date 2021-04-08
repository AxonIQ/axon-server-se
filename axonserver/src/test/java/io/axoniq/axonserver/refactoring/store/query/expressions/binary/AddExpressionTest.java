/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.store.query.expressions.binary;

import io.axoniq.axonserver.refactoring.store.query.Expression;
import io.axoniq.axonserver.refactoring.store.query.ExpressionContext;
import io.axoniq.axonserver.refactoring.store.query.ExpressionResult;
import io.axoniq.axonserver.refactoring.store.query.expressions.Identifier;
import io.axoniq.axonserver.refactoring.store.query.result.NumericExpressionResult;
import org.junit.*;

import static io.axoniq.axonserver.refactoring.store.query.expressions.ResultFactory.*;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class AddExpressionTest {

    private AddExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setup() {
        Expression[] expressions = {
                new Identifier("first"),
                new Identifier("second")
        };
        testSubject = new AddExpression("add", expressions);
        expressionContext = new ExpressionContext();
    }

    @Test
    public void addNumbers() {
        ExpressionResult result =
                testSubject.apply(expressionContext, mapValue("first", numericValue(100), "second", numericValue(200)));
        assertTrue(result instanceof NumericExpressionResult);
        assertEquals(300, result.getNumericValue().longValue());
    }

    @Test
    public void addString() {
        ExpressionResult result = testSubject.apply(expressionContext,
                                                    mapValue("first", stringValue("100"), "second", numericValue(200)));
        assertEquals("100200", result.toString());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void applyWithFirstNull() {
        testSubject.apply(expressionContext, mapValue("first", nullValue(), "second", numericValue(200)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void applyWithInvalidSecond() {
        testSubject.apply(expressionContext, mapValue("first", numericValue(100), "second", stringValue("200")));
    }
}
