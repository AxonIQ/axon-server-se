/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.applicationevents;

import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;

/**
 * Set of application events for specific operations which can be performed on Event Processors. Used to signal other
 * components within an Axon Server cluster that a given operation should be done on an Event Processor.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class EventProcessorEvents {

    public abstract static class BaseEventProcessorsEvent {

        private final boolean proxied;

        public BaseEventProcessorsEvent(boolean proxied) {
            this.proxied = proxied;
        }

        public boolean isProxied() {
            return proxied;
        }
    }

    /**
     * Internal Axon Server event that is published any time is received an update about the Event Processors Status
     * from a client application. This class implements {@link AxonServerEvent} as this event must be forwarded
     * among all instances of AxonServer belonging to the same cluster.
     */
    public static class EventProcessorStatusUpdate implements AxonServerEvent {

        private final ClientEventProcessorInfo eventProcessorStatus;

        /**
         * Creates an instance containing the new status for client's Event Processors.
         *
         * @param clientEventProcessorInfo the updated status for client's event processors.
         */
        public EventProcessorStatusUpdate(ClientEventProcessorInfo clientEventProcessorInfo) {
            this.eventProcessorStatus = clientEventProcessorInfo;
        }

        /**
         * Returns the updated status for client's event processors
         *
         * @return the updated status for client's event processors
         */
        public ClientEventProcessorInfo eventProcessorStatus() {
            return this.eventProcessorStatus;
        }
    }

    public static class EventProcessorStatusUpdated extends BaseEventProcessorsEvent {

        private final ClientEventProcessorInfo eventProcessorStatus;

        public EventProcessorStatusUpdated(ClientEventProcessorInfo eventProcessorStatus, boolean proxied) {
            super(proxied);
            this.eventProcessorStatus = eventProcessorStatus;
        }

        public ClientEventProcessorInfo eventProcessorStatus() {
            return this.eventProcessorStatus;
        }
    }

    public static class PauseEventProcessorRequest extends BaseEventProcessorsEvent {

        private final String clientName;
        private final String processorName;

        public PauseEventProcessorRequest(String clientName, String processorName, boolean proxied) {
            super(proxied);
            this.clientName = clientName;
            this.processorName = processorName;
        }

        public String clientName() {
            return clientName;
        }

        public String processorName() {
            return processorName;
        }
    }


    public static class StartEventProcessorRequest extends BaseEventProcessorsEvent {

        private final String clientName;
        private final String processorName;

        public StartEventProcessorRequest(String clientName, String processorName, boolean proxied) {
            super(proxied);
            this.clientName = clientName;
            this.processorName = processorName;
        }

        public String clientName() {
            return clientName;
        }

        public String processorName() {
            return processorName;
        }
    }

    /**
     * Base for a request to deal with segments. Contains the {@code clientName}, {@code processorName} and
     * {@code segmentId}, to respectively  find the right client, the right processor and the right segment to perform
     * the operation on/with.
     */
    public abstract static class BaseSegmentRequest extends BaseEventProcessorsEvent {

        private final String clientName;
        private final String processorName;
        private final int segmentId;

        /**
         * Instantiate a {@link BaseSegmentRequest} to perform some operation on a specific segment of a given Event
         * Processor.
         *
         * @param proxied       a {@code boolean} specifying whether this message has been proxied yes/no
         * @param clientName    a {@link String} defining the name of the client which should handle this message
         * @param processorName a {@link String} defining the name of the processor which this message should perform
         *                      some operation on/with
         * @param segmentId     an {@code int} specifying the segment identifier which this message should perform some
         *                      operation on/with
         */
        protected BaseSegmentRequest(boolean proxied, String clientName, String processorName, int segmentId) {
            super(proxied);
            this.clientName = clientName;
            this.processorName = processorName;
            this.segmentId = segmentId;
        }

        /**
         * Return the name of the client.
         *
         * @return a {@link String} specifying the name of the client
         */
        public String getClientName() {
            return clientName;
        }

        /**
         * Return the name of the processor.
         *
         * @return a {@link String} specifying the name of the processor
         */
        public String getProcessorName() {
            return processorName;
        }

        /**
         * Return the segment identifier.
         *
         * @return an {@code int} specifying the id of the segment
         */
        public int getSegmentId() {
            return segmentId;
        }
    }

    /**
     * A {@link BaseSegmentRequest} implementation defining the a release segment request for a given
     * {@code processorName}.
     */
    public static class ReleaseSegmentRequest extends BaseSegmentRequest {

        public ReleaseSegmentRequest(String clientName, String processorName, int segmentId, boolean proxied) {
            super(proxied, clientName, processorName, segmentId);
        }
    }

    public static class ProcessorStatusRequest extends BaseEventProcessorsEvent {

        private final String clientName;
        private final String processorName;

        public ProcessorStatusRequest(String clientName, String processorName, boolean proxied) {
            super(proxied);
            this.clientName = clientName;
            this.processorName = processorName;
        }

        public String clientName() {
            return clientName;
        }

        public String processorName() {
            return processorName;
        }
    }

    /**
     * A {@link BaseSegmentRequest} implementation defining the a split segment request for a given
     * {@code processorName}.
     */
    public static class SplitSegmentRequest extends BaseSegmentRequest {

        public SplitSegmentRequest(boolean proxied, String clientName, String processorName, int segmentId) {
            super(proxied, clientName, processorName, segmentId);
        }
    }

    /**
     * A {@link BaseSegmentRequest} implementation defining the a merge segment request for a given
     * {@code processorName}.
     */
    public static class MergeSegmentRequest extends BaseSegmentRequest {

        public MergeSegmentRequest(boolean proxied, String clientName, String processorName, int segmentId) {
            super(proxied, clientName, processorName, segmentId);
        }
    }

    /**
     * Axon Server Event that is published any time a split instruction for
     * a tracking event processor is successfully executed.
     */
    public static class SplitSegmentsSucceeded implements AxonServerEvent {

        private final EventProcessorIdentifier processorIdentifier;

        /**
         * Creates an instance of the event for the specified {@link EventProcessorIdentifier}
         *
         * @param processorIdentifier the identifier of the processor that has been split
         */
        public SplitSegmentsSucceeded(EventProcessorIdentifier processorIdentifier) {
            this.processorIdentifier = processorIdentifier;
        }

        /**
         * Returns the identifier of the processor that has been split
         *
         * @return the identifier of the processor that has been split
         */
        public EventProcessorIdentifier processorIdentifier() {
            return processorIdentifier;
        }
    }

    /**
     * Axon Server Event that is notified any time a merge instruction for
     * a tracking event processor is successfully executed.
     */
    public static class MergeSegmentsSucceeded implements AxonServerEvent {

        private final EventProcessorIdentifier processorIdentifier;

        /**
         * Creates an instance of the event for the specified {@link EventProcessorIdentifier}
         *
         * @param processorIdentifier the identifier of the processor that has been merged
         */
        public MergeSegmentsSucceeded(EventProcessorIdentifier processorIdentifier) {
            this.processorIdentifier = processorIdentifier;
        }

        /**
         * Returns the identifier of the processor that has been merged
         *
         * @return the identifier of the processor that has been merged
         */
        public EventProcessorIdentifier processorIdentifier() {
            return processorIdentifier;
        }
    }
}
