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
import io.axoniq.axonserver.localstorage.query.ExpressionRegistry;
import io.axoniq.axonserver.localstorage.query.PipeExpression;
import io.axoniq.axonserver.localstorage.query.expressions.AbstractExpressionFactory;
import io.axoniq.axonserver.queryparser.QueryElement;

import java.util.Optional;

/**
 * @author Marc Gathier
 */
public class ArithmeticExpressionFactory extends AbstractExpressionFactory {
    public Optional<AbstractArithmeticExpression> doBuild(QueryElement element, ExpressionRegistry registry) {
        switch (element.operator()) {
            case "+":
                return Optional.of(new AddExpression(element.alias().orElse("plus"),
                        buildParameters(element.getParameters(), registry)));
            case "-":
                return Optional.of(new SubtractExpression(element.alias().orElse("subtract"),
                        buildParameters(element.getParameters(), registry)));
            case "*":
                return Optional.of(new MultiplyExpression(element.alias().orElse("multiply"),
                        buildParameters(element.getParameters(), registry)));
            case "/":
                return Optional.of(new DivideExpression(element.alias().orElse("divide"),
                        buildParameters(element.getParameters(), registry)));
            case "%":
                return Optional.of(new ModuloExpression(element.alias().orElse("modulo"),
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
        return Optional.empty();
    }


}
