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

    public static class EventProcessorStatusUpdate extends BaseEventProcessorsEvent {

        private final ClientEventProcessorInfo eventProcessorStatus;

        public EventProcessorStatusUpdate(ClientEventProcessorInfo clientEventProcessorInfo, boolean proxied) {
            super(proxied);
            this.eventProcessorStatus = clientEventProcessorInfo;
        }

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
}
