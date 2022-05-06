/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transformation;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Stores progress while applying a transformation.
 * @author Marc Gathier
 * @since 4.6.0
 */
@Entity
@Table(name="event_store_transformation_progress")
public class EventStoreTransformationProgressJpa {
    @Id
    private String transformationId;
    private long lastSequenceApplied;
    private boolean applied;

    public EventStoreTransformationProgressJpa() {
    }

    public EventStoreTransformationProgressJpa(String transformationId, long lastSequenceApplied, boolean applied) {
        this.transformationId = transformationId;
        this.lastSequenceApplied = lastSequenceApplied;
        this.applied = applied;
    }

    public String getTransformationId() {
        return transformationId;
    }

    public void setTransformationId(String transformationId) {
        this.transformationId = transformationId;
    }

    public long getLastSequenceApplied() {
        return lastSequenceApplied;
    }

    public void setLastSequenceApplied(long lastTokenApplied) {
        this.lastSequenceApplied = lastTokenApplied;
    }

    public boolean isApplied() {
        return applied;
    }

    public void setApplied(boolean completed) {
        this.applied = completed;
    }
}
