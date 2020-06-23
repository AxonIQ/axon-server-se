/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import java.util.Optional;

/**
 * @author Marc Gathier
 */
public interface RemoteAggregateSequenceNumberResolver {

    Optional<Long> getLastSequenceNumber(String context, String aggregateId,
                                         int maxSegments, long maxToken);
}
