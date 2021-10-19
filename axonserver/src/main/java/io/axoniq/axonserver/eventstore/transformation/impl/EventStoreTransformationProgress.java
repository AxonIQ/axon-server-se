/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Entity
@Table(name="event_store_transformation_progress")
public class EventStoreTransformationProgress {
    @Id
    private String transformationId;
    private long lastTokenApplied;
    private boolean completed;

    public EventStoreTransformationProgress(String transformationId) {
        this.transformationId = transformationId;
    }

    public EventStoreTransformationProgress() {
    }

    public String getTransformationId() {
        return transformationId;
    }

    public void setTransformationId(String transformationId) {
        this.transformationId = transformationId;
    }

    public long getLastTokenApplied() {
        return lastTokenApplied;
    }

    public void setLastTokenApplied(long lastTokenApplied) {
        this.lastTokenApplied = lastTokenApplied;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }
}
