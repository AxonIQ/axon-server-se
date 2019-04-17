/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query;

import io.axoniq.axonserver.queryparser.QueryElement;

import java.util.Optional;

public interface ExpressionFactory {

    Optional<Expression> buildExpression(QueryElement element, ExpressionRegistry registry);

    Optional<PipeExpression> buildPipeExpression(QueryElement element, ExpressionRegistry registry);

}
