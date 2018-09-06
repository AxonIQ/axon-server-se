package io.axoniq.axonhub.component.processor;

import io.axoniq.axonhub.component.processor.listener.ClientProcessor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Created by Sara Pellegrini on 26/03/2018.
 * sara.pellegrini@gmail.com
 */
public class EventProcessorMapping implements BiFunction<String, Collection<ClientProcessor>, EventProcessor> {

    @Override
    public EventProcessor apply(String name, Collection<ClientProcessor> clientProcessors){
        String mode = modeOf(clientProcessors);
        if ("Tracking".equals(mode)) {
            return new TrackingProcessor(name, mode, clientProcessors);
        } else {
            return new GenericProcessor(name, mode, clientProcessors);
        }
    }

    private String modeOf(Collection<ClientProcessor> clientProcessors) {
        Set<String> modes = new HashSet<>();
        for (ClientProcessor clientProcessor : clientProcessors) {
            modes.add(clientProcessor.eventProcessorInfo().getMode());
        }
        return modes.size() == 1 ? modes.iterator().next() : "Multiple processing mode detected";
    }


}
