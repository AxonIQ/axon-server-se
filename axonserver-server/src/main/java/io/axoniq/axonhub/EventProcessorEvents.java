package io.axoniq.axonhub;

import io.axoniq.axonhub.internal.grpc.ClientEventProcessorStatus;

/**
 * Created by Sara Pellegrini on 09/04/2018.
 * sara.pellegrini@gmail.com
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

    @KeepNames
    public static class EventProcessorStatusUpdate extends BaseEventProcessorsEvent {

        private final ClientEventProcessorStatus eventProcessorStatus;

        public EventProcessorStatusUpdate(ClientEventProcessorStatus eventProcessorStatus, boolean proxied) {
            super(proxied);
            this.eventProcessorStatus = eventProcessorStatus;
        }

        public ClientEventProcessorStatus eventProcessorStatus(){
            return this.eventProcessorStatus;
        }
    }

    @KeepNames
    public static class EventProcessorStatusUpdated extends BaseEventProcessorsEvent {

        private final ClientEventProcessorStatus eventProcessorStatus;

        public EventProcessorStatusUpdated(ClientEventProcessorStatus eventProcessorStatus, boolean proxied) {
            super(proxied);
            this.eventProcessorStatus = eventProcessorStatus;
        }

        public ClientEventProcessorStatus eventProcessorStatus(){
            return this.eventProcessorStatus;
        }
    }

    @KeepNames
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


    @KeepNames
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

    @KeepNames
    public static class ReleaseSegmentRequest extends BaseEventProcessorsEvent {

        private final String clientName;

        private final String processorName;

        private final int segmentId;

        public ReleaseSegmentRequest(String clientName, String processorName, int segmentId, boolean proxied) {
            super(proxied);
            this.clientName = clientName;
            this.processorName = processorName;
            this.segmentId = segmentId;
        }

        public String clientName() {
            return clientName;
        }

        public String processorName() {
            return processorName;
        }

        public int segmentId() {
            return segmentId;
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

}
