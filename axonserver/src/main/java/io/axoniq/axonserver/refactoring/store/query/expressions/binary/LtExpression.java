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

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class LtExpression extends AbstractBooleanExpression {

    public LtExpression(String alias, Expression[] parameters) {
        super(alias, parameters);
    }

    protected boolean doEvaluate(ExpressionResult first, ExpressionResult second) {
        return Objects.compare(first, second, ExpressionResult::compareTo) < 0;
    }
}
