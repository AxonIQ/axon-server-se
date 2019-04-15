/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.queryparser;

import java.util.Arrays;
import java.util.List;

/**
 * Author: marc
 */
public class BooleanExpr extends BaseQueryElement {
    @Override
    public String toString() {
        return children.get(0).toString() + " " + children.get(1).toString() + " " + children.get(2).toString();
    }

    public PipelineEntry getFirstOperand() {
        return children.get(0);
    }

    public PipelineEntry getSecondOperand() {
        return children.get(2);
    }

    @Override
    public String operator() {
        return String.valueOf(get(1));
    }

    @Override
    public List<? extends QueryElement> getParameters() {
        return Arrays.asList(get(0), get(2));
    }

}
