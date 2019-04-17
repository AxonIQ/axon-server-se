/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.spring;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.function.Consumer;

/**
 * Created by Sara Pellegrini on 13/04/2018.
 * sara.pellegrini@gmail.com
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
