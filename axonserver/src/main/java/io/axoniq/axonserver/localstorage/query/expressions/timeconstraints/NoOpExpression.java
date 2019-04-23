/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.timeconstraints;

import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.PipeExpression;
import io.axoniq.axonserver.localstorage.query.Pipeline;
import io.axoniq.axonserver.localstorage.query.QueryResult;

/**
 * @author Marc Gathier
 * At this moment this is a NoOp. Time constraint is already applied at the source.
 */
public class NoOpExpression implements PipeExpression {

    private static final NoOpExpression INSTANCE = new NoOpExpression();

    public static NoOpExpression instance() {
        return INSTANCE;
    }

    private NoOpExpression() {
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        return next.process(result);
    }

}
