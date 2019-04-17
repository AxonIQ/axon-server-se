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
import io.axoniq.axonserver.localstorage.query.expressions.NumericLiteral;
import org.junit.*;

import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.mapValue;
import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.stringValue;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class RightExpressionTest {
    private RightExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new RightExpression(null, new Expression[] {
                new Identifier("value"),
                new NumericLiteral(null, "2")
                });
        expressionContext = new ExpressionContext();
    }

    @Test
    public void normalRight() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("abcdefg")));
        assertEquals("fg", actual.getValue());
    }

    @Test
    public void tooShort() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("a")));
        assertEquals("a", actual.getValue());
    }

}
