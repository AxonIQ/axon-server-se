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
import org.junit.*;

import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.mapValue;
import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.stringValue;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class MatchExpressionTest {
    private MatchExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new MatchExpression(null, new Expression[] {
                new Identifier("value"),
                new Identifier( "pattern")
                });
        expressionContext = new ExpressionContext();
    }

    @Test
    public void normalMatch() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("abcdefg"),
                "pattern", stringValue("a.*g")));
        assertTrue( actual.isTrue());
    }

    @Test
    public void nonMatch() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("abcdefg"),
                "pattern", stringValue("b.*g")));
        assertFalse( actual.isTrue());
    }

    @Test
    public void nullMatch() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue(
                "pattern", stringValue("b.*g")));
        assertFalse( actual.isTrue());
    }
}
