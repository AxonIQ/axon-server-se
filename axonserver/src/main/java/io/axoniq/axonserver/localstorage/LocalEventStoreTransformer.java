/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.localstorage.file.TransformationProgress;
import reactor.core.publisher.Flux;

/**
 * @author Marc Gathier
 * @since
 */
public interface LocalEventStoreTransformer {

    void deleteOldVersions(String context, int version);

    void rollbackSegments(String context, int version);

    Flux<TransformationProgress> transformEvents(String context, long firstToken, long lastToken,
                                                 boolean keepOldVersions,
                                                 int version,
                                                 EventTransformationFunction transformationFunction);
}
