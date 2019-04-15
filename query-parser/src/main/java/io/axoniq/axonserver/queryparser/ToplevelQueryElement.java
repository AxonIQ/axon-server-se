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
 * Author: marc
 */
public class ToplevelQueryElement extends BaseQueryElement {

    @Override
    public String toString() {
        return String.join(" or ", children.stream().map(PipelineEntry::toString).collect(Collectors.toList()));
    }

    @Override
    public String operator() {
        if (children.size() == 1) {
            return children.get(0).operator();
        }
        return "or";
    }

    @Override
    public List<? extends QueryElement> getParameters() {
        if (children.size() == 1) {
            return children.get(0).getParameters();
        }
        return children;
    }

}
