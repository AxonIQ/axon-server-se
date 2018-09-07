package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;

import java.util.Collection;

/**
 * Created by Sara Pellegrini on 26/03/2018.
 * sara.pellegrini@gmail.com
 */
public class GenericProcessor implements EventProcessor {

    private final String name;

    private final String mode;

    private final Collection<ClientProcessor> processors;

    public GenericProcessor(String name,
                            String mode, Collection<ClientProcessor> processors) {
        this.name = name;
        this.mode = mode;
        this.processors = processors;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String mode() {
        return mode;
    }

    protected Collection<ClientProcessor> processors(){
        return this.processors;
    }

}
