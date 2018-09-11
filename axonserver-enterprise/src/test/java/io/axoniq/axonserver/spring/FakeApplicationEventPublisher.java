package io.axoniq.axonserver.spring;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public class FakeApplicationEventPublisher implements ApplicationEventPublisher {

    private final Collection<Consumer<Object>> consumers = new LinkedList<>();

    private final Collection<Object> events = new LinkedList<>();

    @Override
    public void publishEvent(ApplicationEvent event) {
        events.add(event);
        consumers.forEach(consumer -> consumer.accept(event));
    }

    @Override
    public void publishEvent(Object event) {
        events.add(event);
        consumers.forEach(consumer -> consumer.accept(event));
    }

    public Iterable<Object> events(){
        return Collections.unmodifiableCollection(events);
    }

    public void add(Consumer<Object> consumer){
        consumers.add(consumer);
    }
}
