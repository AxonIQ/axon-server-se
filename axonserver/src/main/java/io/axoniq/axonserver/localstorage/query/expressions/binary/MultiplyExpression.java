/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;

/**
 * @author Marc Gathier
 */
public class MultiplyExpression extends AbstractArithmeticExpression {

    public MultiplyExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    @Override
    protected ExpressionResult doCompute(ExpressionResult first, ExpressionResult second) {
        return first.multiply(second);
    }

}
