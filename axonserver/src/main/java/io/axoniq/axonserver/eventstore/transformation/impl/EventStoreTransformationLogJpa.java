/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Entity
@Table(name = "event_store_transformation_logs")
public class EventStoreTransformationLogJpa {

    @Id
    @GeneratedValue
    private Long id;

    private Long segmentId;
    private Integer version;

    @ManyToOne
    @JoinColumn(name = "transformation_id")
    @JsonIgnore
    private EventStoreTransformationJpa transformation;


    public EventStoreTransformationLogJpa( Long segmentId, Integer version) {
        this.segmentId = segmentId;
        this.version = version;
    }

    public EventStoreTransformationLogJpa() {
    }

    public Long getId() {
        return id;
    }

    public Long getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(Long segmentId) {
        this.segmentId = segmentId;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public EventStoreTransformationJpa getTransformation() {
        return transformation;
    }

    public void setTransformation(EventStoreTransformationJpa transformation) {
        this.transformation = transformation;
    }
}
