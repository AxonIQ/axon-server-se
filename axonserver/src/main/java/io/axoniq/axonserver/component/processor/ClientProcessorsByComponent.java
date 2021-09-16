/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.balancing.SameProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.function.Predicate;

import static java.util.stream.StreamSupport.stream;

/**
 * {@link ClientProcessors} implementation responsible to provide all known {@link ClientProcessor}s that are defined
 * in a specific client application. Please note that it provides not only the {@link ClientProcessor}s active in the
 * specified client application, but also the {@link ClientProcessor}s active in any client that have the same name and
 * token identifier of {@link ClientProcessor} active in the specified client application.
 *
 * <p>
 * To explain better what <i> a {@link ClientProcessor} defined in the correct client application</i> means,
 * see the following example related to the <b>componentA</b>.
 * </p>
 * If these are all known client event processor instances:
 * <ul>
 * <li> componentA - processorBlue - tokenStore1
 * <li> componentB - processorWhite - tokenStore1
 * <li> componentA - processorWhite - tokenStore1
 * <li> componentC - processorRed - tokenStore2
 * <li> componentA - processorRed - tokenStore1
 * <li> componentB - processorGreen - tokenStore1
 * </ul>
 * this implementation will provide the only following items:
 * <ul>
 * <li> componentA - processorBlue - tokenStore1
 * <li> componentB - processorWhite - tokenStore1
 * <li> componentA - processorWhite - tokenStore1
 * <li> componentA - processorRed - tokenStore1
 * </ul>
 * In other words, it provides all instances in the componentA plus
 * <ul>
 * <li> componentB - processorWhite - tokenStore1
 * </ul>
 * that is the only one not part of <b>componentA</b> that has same name/tokenStore of one of the processors defined
 * from <b>componentA</b>.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public class ClientProcessorsByComponent implements ClientProcessors {

    private final ClientProcessors allEventProcessors;

    private final Predicate<ClientProcessor> existInComponent;

    /**
     * Creates an instance defined by the full list of all {@link ClientProcessor}s and the component name
     *
     * @param allEventProcessors all known {@link ClientProcessor}s
     * @param component          the component name of the client application
     */
    ClientProcessorsByComponent(ClientProcessors allEventProcessors,
                                String component) {
        this(allEventProcessors, new ExistsInComponent(component, allEventProcessors));
    }

    /**
     * Creates an instance defined by the full list of all {@link ClientProcessor}s and a predicate to filter
     * the {@link ClientProcessor}s defined in the correct client application.
     *
     * @param allEventProcessors all known {@link ClientProcessor}s
     * @param existInComponent   the predicate to test if a {@link ClientProcessor} is defined in the client application
     */
    private ClientProcessorsByComponent(ClientProcessors allEventProcessors,
                                        Predicate<ClientProcessor> existInComponent) {
        this.allEventProcessors = allEventProcessors;
        this.existInComponent = existInComponent;
    }

    @NotNull
    @Override
    public Iterator<ClientProcessor> iterator() {
        return stream(allEventProcessors.spliterator(), false)
                .filter(existInComponent)
                .iterator();
    }

    private static final class ExistsInComponent implements Predicate<ClientProcessor> {

        /* Iterable of all Client Processors defined directly in the specified component*/
        private final Iterable<ClientProcessor> allEventProcessors;
        private final String component;

        ExistsInComponent(String component, ClientProcessors allEventProcessors) {
            this.allEventProcessors = allEventProcessors;
            this.component = component;
        }

        @Override
        public boolean test(ClientProcessor processor) {
            return stream(allEventProcessors.spliterator(), false)
                    .filter(p -> p.belongsToComponent(component))
                    .anyMatch(p -> new SameProcessor(p).test(processor));
        }
    }
}
