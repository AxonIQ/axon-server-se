/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.jpa;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Stores progress while applying a transformation.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
@Entity
@Table(name = "et_local_event_store_transformation")
public class LocalEventStoreTransformationJpa {

    @Id
    private String transformationId;
    private long lastSequenceApplied;
    private boolean applied;

    public LocalEventStoreTransformationJpa() {
    }

    public LocalEventStoreTransformationJpa(String transformationId,
                                            long lastSequenceApplied,
                                            boolean applied) {
        this.transformationId = transformationId;
        this.lastSequenceApplied = lastSequenceApplied;
        this.applied = applied;
    }

    public String getTransformationId() {
        return transformationId;
    }

    public long getLastSequenceApplied() {
        return lastSequenceApplied;
    }

    public boolean isApplied() {
        return applied;
    }
}
