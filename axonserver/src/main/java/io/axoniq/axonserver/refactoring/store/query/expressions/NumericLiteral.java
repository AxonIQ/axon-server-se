/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.store.query.expressions;

import io.axoniq.axonserver.refactoring.store.query.Expression;
import io.axoniq.axonserver.refactoring.store.query.ExpressionContext;
import io.axoniq.axonserver.refactoring.store.query.ExpressionResult;
import io.axoniq.axonserver.refactoring.store.query.result.NumericExpressionResult;

/**
 * @author Marc Gathier
 */
public class NumericLiteral implements Expression {

    private final String alias;
    private final String literal;

    public NumericLiteral(String alias, String literal) {
        this.alias = alias;
        this.literal = literal;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult data) {
        return new NumericExpressionResult(literal);
    }

    @Override
    public String alias() {
        return alias;
    }
}
