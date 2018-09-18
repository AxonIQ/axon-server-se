package io.axoniq.axonserver.component.processor.listener;

import io.axoniq.platform.grpc.EventProcessorInfo;

/**
 * Created by Sara Pellegrini on 30/03/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeClientProcessor implements ClientProcessor {

    private final String clientId;

    private final boolean belongsToComponent;

    private final EventProcessorInfo eventProcessorInfo;

    public FakeClientProcessor(String clientId, boolean belongsToComponent,
                               EventProcessorInfo eventProcessorInfo) {
        this.clientId = clientId;
        this.belongsToComponent = belongsToComponent;
        this.eventProcessorInfo = eventProcessorInfo;
    }

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public EventProcessorInfo eventProcessorInfo() {
        return eventProcessorInfo;
    }

    @Override
    public Boolean belongsToComponent(String component) {
        return belongsToComponent;
    }

    @Override
    public boolean belongsToContext(String context) {
        return true;
    }
}
