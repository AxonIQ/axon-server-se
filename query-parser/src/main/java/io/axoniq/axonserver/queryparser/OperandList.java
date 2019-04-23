/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.queryparser;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 * @since 4.0
 */
public class OperandList extends BaseQueryElement {

    @Override
    public String toString() {
        return "[ " +
                String.join(", ", children.stream().map(Object::toString).collect(Collectors.toList()))
                + "]";
    }

    @Override
    public String operator() {
        return "list";
    }

    @Override
    public List<? extends QueryElement> getParameters() {
        return children;
    }

    @Override
    public String getLiteral() {
        return "list";
    }
}
