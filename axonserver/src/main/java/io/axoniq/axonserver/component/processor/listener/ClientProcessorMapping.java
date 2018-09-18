package io.axoniq.axonserver.component.processor.listener;

import io.axoniq.platform.grpc.EventProcessorInfo;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface ClientProcessorMapping {

    default ClientProcessor map(String clientId, String component, String context, EventProcessorInfo eventProcessorInfo){
        return new ClientProcessor() {

            @Override
            public String clientId() {
                return clientId;
            }

            @Override
            public EventProcessorInfo eventProcessorInfo() {
                return eventProcessorInfo;
            }

            @Override
            public Boolean belongsToComponent(String c) {
                return c.equals(component);
            }

            @Override
            public boolean belongsToContext(String c) {
                return c.equals(context);
            }
        };
    }

}
