/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.jpa;

import java.util.Date;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * Stores the information on each transformation.
 * @author Marc Gathier
 * @since 4.6.0
 */
@Entity
@Table(name = "et_event_store_transformation")
public class EventStoreTransformationJpa {

    public enum Status {
        ACTIVE,
        CANCELLED, // final state
        APPLYING,
        APPLIED, // final state
        FAILED // final state
    }

    @Id
    private String transformationId;
    private String context;

    @Enumerated(EnumType.STRING)
    private Status status;

    private int version;

    private String description;

    @Temporal(TemporalType.TIMESTAMP)
    private Date dateApplied;

    private String applier;

    // TODO: 12/28/21 name
    private Long lastSequence;

    private Long lastEventToken;

    public EventStoreTransformationJpa() {
    }

    public EventStoreTransformationJpa(String transformationId,
                                       String description,
                                       String context,
                                       int version) {
        this.transformationId = transformationId;
        this.context = context;
        this.version = version;
        this.description = description;
        this.status = Status.ACTIVE;
    }

    public EventStoreTransformationJpa(EventStoreTransformationJpa original) {
        this.transformationId = original.transformationId;
        this.applier = original.applier;
        this.context = original.context;
        this.dateApplied = original.dateApplied;
        this.description = original.description;
        this.lastSequence = original.lastSequence;
        this.lastEventToken = original.lastEventToken;
        this.status = original.status;
        this.version = original.version;
    }

    public String transformationId() {
        return transformationId;
    }

    public void setTransformationId(String transformationId) {
        this.transformationId = transformationId;
    }

    public String context() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public Status status() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int version() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String description() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date dateApplied() {
        return dateApplied;
    }

    public void setDateApplied(Date dateApplied) {
        this.dateApplied = dateApplied;
    }

    public String applier() {
        return applier;
    }

    public void setApplier(String appliedBy) {
        this.applier = appliedBy;
    }

    public Long lastSequence() {
        return lastSequence;
    }

    public void setLastSequence(Long lastTransformationEntrySequence) {
        this.lastSequence = lastTransformationEntrySequence;
    }

    public Long lastEventToken() {
        return lastEventToken;
    }

    public void setLastEventToken(Long lastEventToken) {
        this.lastEventToken = lastEventToken;
    }


    @Override
    public String toString() {
        return "EventStoreTransformationJpa{" +
                "transformationId='" + transformationId + '\'' +
                ", context='" + context + '\'' +
                ", status=" + status +
                ", version=" + version +
                ", description='" + description + '\'' +
                ", dateApplied=" + dateApplied +
                ", applier='" + applier + '\'' +
                ", lastSequence=" + lastSequence +
                ", lastEventToken=" + lastEventToken +
                '}';
    }
}
