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
import java.util.Optional;

/**
 * @author Marc Gathier
 * @since 4.0
 */
public interface QueryElement {

    String operator();

    List<? extends QueryElement> getParameters();

    String getLiteral();

    Optional<String> alias();
}
