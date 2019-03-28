/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.result;

import com.fasterxml.jackson.annotation.JsonValue;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import net.minidev.json.JSONObject;

/**
 * @author Marc Gathier
 */
public class JSONExpressionResult implements AbstractMapExpressionResult  {
    private final JSONObject value;

    public JSONExpressionResult(JSONObject value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public int compareTo( ExpressionResult o) {
        return value.toJSONString().compareTo(o.toString());
    }

    @Override
    @JsonValue
    public String toString() {
        return value.toJSONString();
    }

    @Override
    public Iterable<String> getColumnNames() {
        return value.keySet();
    }

}
