/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import java.util.Date;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Entity
@Table(name = "event_store_transformations")
public class EventStoreTransformationJpa {

    public boolean isKeepOldVersions() {
        return keepOldVersions;
    }

    public void setKeepOldVersions(boolean keepOldVersions) {
        this.keepOldVersions = keepOldVersions;
    }

    public enum Status {
        CREATED,
        CLOSED,
        DONE,
        FAILED
    }
    @Id
    private String transformationId;
    private String context;

    @Enumerated(EnumType.ORDINAL)
    private Status status;

    private boolean keepOldVersions;

    private int version;

    private String description;

    @Temporal(TemporalType.TIMESTAMP)
    private Date dateApplied;

    private String appliedBy;


    public EventStoreTransformationJpa(String transformationId, String context) {
        this.transformationId = transformationId;
        this.context = context;
        this.status = Status.CREATED;
    }

    public EventStoreTransformationJpa() {
    }

    public String getTransformationId() {
        return transformationId;
    }

    public void setTransformationId(String transformationId) {
        this.transformationId = transformationId;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getDateApplied() {
        return dateApplied;
    }

    public void setDateApplied(Date dateApplied) {
        this.dateApplied = dateApplied;
    }

    public String getAppliedBy() {
        return appliedBy;
    }

    public void setAppliedBy(String appliedBy) {
        this.appliedBy = appliedBy;
    }
}
