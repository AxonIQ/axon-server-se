/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.metric.MeterFactory;

import java.util.Map;

/**
 * @author Marc Gathier
 */
public class StatusInfo {
    private MeterFactory.RateMeter commandRate;
    private MeterFactory.RateMeter queryRate;
    private MeterFactory.RateMeter eventRate;
    private MeterFactory.RateMeter snapshotRate;
    private long nrOfEvents;
    private long nrOfSnapshots;
    private long nrOfSubscriptionQueries;
    private long nrOfActiveSubscriptionQueries;
    private long nrOfSubscriptionQueriesUpdates;
    private Map<String, Iterable<Long>> eventTrackers;

    public MeterFactory.RateMeter getCommandRate() {
        return commandRate;
    }

    public void setCommandRate(MeterFactory.RateMeter commandRate) {
        this.commandRate = commandRate;
    }

    public MeterFactory.RateMeter getQueryRate() {
        return queryRate;
    }

    public void setQueryRate(MeterFactory.RateMeter queryRate) {
        this.queryRate = queryRate;
    }

    public long getNrOfEvents() {
        return nrOfEvents;
    }

    public void setNrOfEvents(long nrOfEvents) {
        this.nrOfEvents = nrOfEvents;
    }

    public long getNrOfSnapshots() {
        return nrOfSnapshots;
    }

    public void setNrOfSnapshots(long nrOfSnapshots) {
        this.nrOfSnapshots = nrOfSnapshots;
    }

    public void setEventTrackers(Map<String, Iterable<Long>> eventTrackers) {
        this.eventTrackers = eventTrackers;
    }

    public Map<String, Iterable<Long>> getEventTrackers() {
        return eventTrackers;
    }

    public void setNrOfSubscriptionQueries(long nrOfSubscriptionQueries) {
        this.nrOfSubscriptionQueries = nrOfSubscriptionQueries;
    }

    public void setNrOfActiveSubscriptionQueries(long nrOfActiveSubscriptionQueries) {
        this.nrOfActiveSubscriptionQueries = nrOfActiveSubscriptionQueries;
    }

    public long getNrOfSubscriptionQueries() {
        return nrOfSubscriptionQueries;
    }

    public long getNrOfActiveSubscriptionQueries() {
        return nrOfActiveSubscriptionQueries;
    }

    public long getNrOfSubscriptionQueriesUpdates() {
        return nrOfSubscriptionQueriesUpdates;
    }

    public void setNrOfSubscriptionQueriesUpdates(long nrOfSubscriptionQueriesUpdates) {
        this.nrOfSubscriptionQueriesUpdates = nrOfSubscriptionQueriesUpdates;
    }

    public MeterFactory.RateMeter getSnapshotRate() {
        return snapshotRate;
    }

    public void setSnapshotRate(MeterFactory.RateMeter snapshotRate) {
        this.snapshotRate = snapshotRate;
    }

    public MeterFactory.RateMeter getEventRate() {
        return eventRate;
    }

    public void setEventRate(MeterFactory.RateMeter eventRate) {
        this.eventRate = eventRate;
    }
}
