package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;

/**
 * Author: marc
 */
public class ClientEventProcessorInfo {

    private final String clientName;
    private final String context;
    private final EventProcessorInfo eventProcessorInfo;

    public ClientEventProcessorInfo(String clientName, String context, EventProcessorInfo eventProcessorInfo) {

        this.clientName = clientName;
        this.context = context;
        this.eventProcessorInfo = eventProcessorInfo;
    }

    public String getClientName() {
        return clientName;
    }

    public String getContext() {
        return context;
    }

    public EventProcessorInfo getEventProcessorInfo() {
        return eventProcessorInfo;
    }
}
