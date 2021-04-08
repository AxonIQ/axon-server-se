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
import io.axoniq.axonserver.refactoring.store.query.ExpressionResult;

/**
 * @author Marc Gathier
 */
public class SubtractExpression extends AbstractArithmeticExpression {

    public SubtractExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    @Override
    protected ExpressionResult doCompute(ExpressionResult first, ExpressionResult second) {
        return first.subtract(second);
    }
}
