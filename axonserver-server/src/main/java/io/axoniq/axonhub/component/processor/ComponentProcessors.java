package io.axoniq.axonhub.component.processor;

import io.axoniq.axonhub.component.ComponentItems;
import io.axoniq.axonhub.component.processor.listener.ClientProcessor;
import io.axoniq.axonhub.component.processor.listener.ClientProcessors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ComponentProcessors implements Iterable<EventProcessor> {

    private final String component;

    private final ClientProcessors eventProcessors;

    private final EventProcessorMapping mapping;
    private final String context;

    public ComponentProcessors(String component, String context,
                               ClientProcessors eventProcessors) {
        this(component, context, eventProcessors, new EventProcessorMapping());
    }

    public ComponentProcessors(String component, String context,
                               ClientProcessors eventProcessors,
                               EventProcessorMapping mapping) {
        this.component = component;
        this.eventProcessors = eventProcessors;
        this.mapping = mapping;
        this.context = context;
    }

    @Override
    public Iterator<EventProcessor> iterator() {
        ComponentItems<ClientProcessor> processors = new ComponentItems<>(component, context, eventProcessors);

        //group by processorName
        Map<String, Set<ClientProcessor>> processorsMap = new HashMap<>();
        for (ClientProcessor processor : processors) {
            String processorName = processor.eventProcessorInfo().getProcessorName();
            Set<ClientProcessor> clientProcessors = processorsMap.computeIfAbsent(processorName, name -> new HashSet<>());
            clientProcessors.add(processor);
        }

        return processorsMap.entrySet().stream().map(
                entry -> mapping.apply(
                        entry.getKey(),
                        entry.getValue())
        ).iterator();
    }
}
