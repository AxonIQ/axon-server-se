/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.expressions.binary;

import io.axoniq.axonserver.queryparser.QueryElement;
import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionRegistry;
import io.axoniq.axonserver.localstorage.query.PipeExpression;
import io.axoniq.axonserver.localstorage.query.expressions.AbstractExpressionFactory;

import java.util.Optional;

public class BinaryExpressionFactory extends AbstractExpressionFactory {
    public Optional<AbstractBooleanExpression> doBuild(QueryElement element, ExpressionRegistry registry) {
        switch (element.operator()) {
            case "=":
                return Optional.of(new EqExpression(element.alias().orElse("eq"),
                                                    buildParameters(element.getParameters(), registry)));
            case "!=":
                return Optional.of(new NotEqExpression(element.alias().orElse("neq"),
                                                       buildParameters(element.getParameters(), registry)));
            case ">":
                return Optional.of(new GtExpression(element.alias().orElse("gt"),
                                                    buildParameters(element.getParameters(), registry)));
            case ">=":
                return Optional.of(new GtEqExpression(element.alias().orElse("gteq"),
                        buildParameters(element.getParameters(), registry)));
            case "<":
                return Optional.of(new LtExpression(element.alias().orElse("lt"),
                                                    buildParameters(element.getParameters(), registry)));
            case "<=":
                return Optional.of(new LtEqExpression(element.alias().orElse("lteq"),
                        buildParameters(element.getParameters(), registry)));
            case "in":
                return Optional.of(new InExpression(element.alias().orElse("in"),
                        buildParameters(element.getParameters(), registry)));
            default:
        }
        return Optional.empty();
    }

    @Override
    public Optional<Expression> buildExpression(QueryElement element, ExpressionRegistry registry) {
        return doBuild(element, registry).map(e -> (Expression)e);
    }

    @Override
    public Optional<PipeExpression> buildPipeExpression(QueryElement element, ExpressionRegistry registry) {
        return doBuild(element, registry).map(e -> (PipeExpression)e);
    }



}
