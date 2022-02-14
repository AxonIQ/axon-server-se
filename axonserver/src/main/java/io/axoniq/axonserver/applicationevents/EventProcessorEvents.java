/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.applicationevents;

import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;

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

        private final String context;
        private final String clientId;
        private final String processorName;
        private final String instructionId;

        public PauseEventProcessorRequest(String context, String clientId, String processorName,
                                          String instructionId, boolean proxied) {
            super(proxied);
            this.context = context;
            this.clientId = clientId;
            this.processorName = processorName;
            this.instructionId = instructionId;
        }

        public String clientId() {
            return clientId;
        }

        public String processorName() {
            return processorName;
        }

        /**
         * Returns the principal context of the event processor
         *
         * @return the principal context of the event processor
         */
        public String context() {
            return context;
        }

        public String instructionId() {
            return instructionId;
        }
    }


    public static class StartEventProcessorRequest extends BaseEventProcessorsEvent {

        private final String context;
        private final String clientId;
        private final String processorName;
        private final String instructionId;

        public StartEventProcessorRequest(String context, String clientId, String processorName,
                                          String instructionId, boolean proxied) {
            super(proxied);
            this.context = context;
            this.clientId = clientId;
            this.processorName = processorName;
            this.instructionId = instructionId;
        }

        public String clientId() {
            return clientId;
        }

        public String processorName() {
            return processorName;
        }

        /**
         * Returns the principal context of the event processor
         *
         * @return the principal context of the event processor
         */
        public String context() {
            return context;
        }

        public String instructionId() {
            return instructionId;
        }
    }

    /**
     * Base for a request to deal with segments. Contains the {@code clientId}, {@code processorName} and
     * {@code segmentId}, to respectively  find the right client, the right processor and the right segment to perform
     * the operation on/with.
     */
    public abstract static class BaseSegmentRequest extends BaseEventProcessorsEvent {

        private final String context;
        private final String clientId;
        private final String processorName;
        private final String instructionId;
        private final int segmentId;

        /**
         * Instantiate a {@link BaseSegmentRequest} to perform some operation on a specific segment of a given Event
         * Processor.
         *
         * @param proxied       a {@code boolean} specifying whether this message has been proxied yes/no
         * @param context       the principal context of the event processor
         * @param clientId      a {@link String} defining the platform stream of the client which should handle this
         *                      message
         * @param instructionId a unique identifier for the instruction
         * @param processorName a {@link String} defining the name of the processor which this message should perform
         *                      some operation on/with
         * @param segmentId     an {@code int} specifying the segment identifier which this message should perform some
         */
        protected BaseSegmentRequest(boolean proxied, String context, String clientId, String instructionId,
                                     String processorName, int segmentId) {
            super(proxied);
            this.context = context;
            this.clientId = clientId;
            this.instructionId = instructionId;
            this.processorName = processorName;
            this.segmentId = segmentId;
        }

        /**
         * Return the name of the client.
         *
         * @return a {@link String} specifying the name of the client
         */
        public String getClientId() {
            return clientId;
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

        /**
         * Returns the principal context of the event processor
         *
         * @return the principal context of the event processor
         */
        public String context() {
            return context;
        }

        public String instructionId() {
            return instructionId;
        }
    }

    /**
     * A {@link BaseSegmentRequest} implementation defining the a release segment request for a given
     * {@code processorName}.
     */
    public static class ReleaseSegmentRequest extends BaseSegmentRequest {

        public ReleaseSegmentRequest(String context, String clientName, String processorName, int segmentId,
                                     String instructionId, boolean proxied) {
            super(proxied, context, clientName, instructionId, processorName, segmentId);
        }
    }

    public static class ProcessorStatusRequest extends BaseEventProcessorsEvent {

        private final String context;
        private final String clientId;
        private final String processorName;

        public ProcessorStatusRequest(String context, String clientId, String processorName, boolean proxied) {
            super(proxied);
            this.context = context;
            this.clientId = clientId;
            this.processorName = processorName;
        }

        public String clientId() {
            return clientId;
        }

        public String processorName() {
            return processorName;
        }

        /**
         * Returns the principal context of the event processor
         * @return the principal context of the event processor
         */
        public String context() {
            return context;
        }
    }

    /**
     * A {@link BaseSegmentRequest} implementation defining the a split segment request for a given
     * {@code processorName}.
     */
    public static class SplitSegmentRequest extends BaseSegmentRequest {

        public SplitSegmentRequest(boolean proxied, String context, String clientId, String processorName,
                                   int segmentId, String instructionId) {
            super(proxied, context, clientId, instructionId, processorName, segmentId);
        }
    }

    /**
     * A {@link BaseSegmentRequest} implementation defining the a merge segment request for a given
     * {@code processorName}.
     */
    public static class MergeSegmentRequest extends BaseSegmentRequest {

        public MergeSegmentRequest(boolean proxied, String context, String clientId, String processorName,
                                   int segmentId, String instructionId) {
            super(proxied, context, clientId, instructionId, processorName, segmentId);
        }
    }

    /**
     * Axon Server Event that is published any time a split instruction for
     * a tracking event processor is successfully executed.
     */
    public static class SplitSegmentsSucceeded implements AxonServerEvent {

        private final String context;

        private final String clientId;

        private final String processorName;

        /**
         * Creates an instance of the event for the specified client and processor
         *
         * @param context       the primary context of the client application
         * @param clientId    the id of the client that performed the split operation
         * @param processorName the name of the processor that has been split
         */
        public SplitSegmentsSucceeded(String context, String clientId, String processorName) {
            this.context = context;
            this.clientId = clientId;
            this.processorName = processorName;
        }

        /**
         * Returns the name of the client that performed the split operation
         *
         * @return the name of the client that performed the split operation
         */
        public String clientId() {
            return clientId;
        }

        /**
         * Returns the name of the processor that has been split
         *
         * @return the name of the processor that has been split
         */
        public String processorName() {
            return processorName;
        }

        /**
         * Returns the principal context of the event processor
         * @return the principal context of the event processor
         */
        public String context() {
            return context;
        }
    }

    /**
     * Axon Server Event that is notified any time a merge instruction for
     * a tracking event processor is successfully executed by a client.
     */
    public static class MergeSegmentsSucceeded implements AxonServerEvent {

        private final String context;

        private final String clientId;

        private final String processorName;

        /**
         * Creates an instance of the event for the specified client and processor
         *
         * @param context       the primary context of the client application
         * @param clientId    the id of the client that performed the merge operation
         * @param processorName the name of the processor that has been merged
         */
        public MergeSegmentsSucceeded(String context, String clientId, String processorName) {
            this.context = context;
            this.clientId = clientId;
            this.processorName = processorName;
        }

        /**
         * Returns the name of the client that performed the merge operation
         *
         * @return the name of the client that performed the merge operation
         */
        public String clientId() {
            return clientId;
        }

        /**
         * Returns the name of the processor that has been merged
         *
         * @return the name of the processor that has been merged
         */
        public String processorName() {
            return processorName;
        }

        /**
         * Returns the principal context of the event processor
         * @return the principal context of the event processor
         */
        public String context() {
            return context;
        }
    }
}
