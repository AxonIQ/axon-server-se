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
import org.junit.*;

import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.mapValue;
import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.stringValue;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class LowerExpressionTest {
    private LowerExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new LowerExpression(null,
                new Identifier("value")
                );
        expressionContext = new ExpressionContext();
    }

    @Test
    public void lower() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("UpperCase")));
        assertEquals("uppercase", actual.getValue());
    }
    @Test
    public void lowerNull() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value123", stringValue("UpperCase")));
        assertNull( actual.getValue());
    }
}
