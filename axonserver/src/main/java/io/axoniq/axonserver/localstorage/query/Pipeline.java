/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query;

import java.util.List;

public interface Pipeline {

    boolean process(QueryResult value);

    default List<String> columnNames(List<String> inputColumnNames) {
        return inputColumnNames;
    }
}
