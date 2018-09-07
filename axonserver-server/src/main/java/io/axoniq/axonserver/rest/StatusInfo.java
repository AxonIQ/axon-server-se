package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;

import java.util.Map;

/**
 * Author: marc
 */
@KeepNames
public class StatusInfo {
    private long nrOfCommands;
    private long nrOfQueries;
    private long nrOfEvents;
    private long nrOfSnapshots;
    private long nrOfSubscriptionQueries;
    private long nrOfActiveSubscriptionQueries;
    private long nrOfSubscriptionQueriesUpdates;
    private Map<String, Iterable<Long>> eventTrackers;

    public long getNrOfCommands() {
        return nrOfCommands;
    }

    public void setNrOfCommands(long nrOfCommands) {
        this.nrOfCommands = nrOfCommands;
    }

    public long getNrOfQueries() {
        return nrOfQueries;
    }

    public void setNrOfQueries(long nrOfQueries) {
        this.nrOfQueries = nrOfQueries;
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
}
