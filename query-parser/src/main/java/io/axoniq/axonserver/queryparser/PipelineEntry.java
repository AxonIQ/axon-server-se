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

/**
 * @author Marc Gathier
 * @since 4.0
 */
public interface PipelineEntry extends QueryElement {

    void add(PipelineEntry pipelineEntry);

    PipelineEntry get(int idx);

    List<String> getIdentifiers();

}
