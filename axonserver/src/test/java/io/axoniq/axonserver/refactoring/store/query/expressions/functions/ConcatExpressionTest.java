/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.store.query.expressions.functions;

import io.axoniq.axonserver.refactoring.store.query.Expression;
import io.axoniq.axonserver.refactoring.store.query.ExpressionContext;
import io.axoniq.axonserver.refactoring.store.query.ExpressionResult;
import io.axoniq.axonserver.refactoring.store.query.expressions.Identifier;
import io.axoniq.axonserver.refactoring.store.query.expressions.StringLiteral;
import org.junit.*;

import static io.axoniq.axonserver.refactoring.store.query.expressions.ResultFactory.*;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class ConcatExpressionTest {

    private ConcatExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new ConcatExpression(null, new Expression[]{
                new Identifier("value"),
                new StringLiteral("---")
        });
        expressionContext = new ExpressionContext();
    }

    @Test
    public void concat() {
        ExpressionResult actual = testSubject.apply(expressionContext,
                                                    mapValue("value",
                                                             listValue(stringValue("a"),
                                                                       stringValue("b"),
                                                                       numericValue(12))));
        assertEquals("a---b---12", actual.getValue());
    }
}
