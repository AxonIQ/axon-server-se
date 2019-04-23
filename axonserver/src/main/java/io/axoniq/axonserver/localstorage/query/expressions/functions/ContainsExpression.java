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
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.expressions.binary.AbstractBooleanExpression;
import io.axoniq.axonserver.localstorage.query.result.ListExpressionResult;

/**
 * @author Marc Gathier
 */
public class ContainsExpression extends AbstractBooleanExpression {

    public ContainsExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    @Override
    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        String toSearch = String.valueOf(second.getValue());
        if (first instanceof ListExpressionResult) {
            return ((ListExpressionResult) first).getValue().stream()
                                                 .anyMatch(er -> toSearch.equals(String.valueOf(er.getValue())));
        }
        return String.valueOf(first.getValue()).contains(toSearch);
    }
}
