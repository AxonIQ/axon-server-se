/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/**
 * Iterable of {@link ClientProcessor}s names.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public class ProcessorNames implements Iterable<String> {

    private final Iterable<ClientProcessor> clientProcessors;

    /**
     * Creates an instance for the specified clientProcessors.
     *
     * @param clientProcessors {@link ClientProcessor}s
     */
    public ProcessorNames(Iterable<ClientProcessor> clientProcessors) {
        this.clientProcessors = clientProcessors;
    }

    @NotNull
    @Override
    public Iterator<String> iterator() {
        Iterator<ClientProcessor> iterator = clientProcessors.iterator();
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public String next() {
                return iterator.next().eventProcessorInfo().getProcessorName();
            }
        };
    }
}
