/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.ComponentItems;
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
 * specified client application, but also the {@link ClientProcessor}s active in any client that have the same context
 * and name of {@link ClientProcessor} active in the specified client application.
 *
 * <p>
 * To explain better what <i> a {@link ClientProcessor} defined in the correct client application</i> means,
 * see the following example related to the <b>componentA</b> in <b>context1</b>.
 * </p>
 * If these are all known client event processor instances:
 * <ul>
 * <li> componentA - context1 - processorBlue
 * <li> componentB - context1 - processorWhite
 * <li> componentA - context1 - processorWhite
 * <li> componentC - context2 - processorRed
 * <li> componentA - context1 - processorRed
 * <li> componentB - context1 - processorGreen
 * </ul>
 * this implementation will provide the only following items:
 * <ul>
 * <li> componentA - context1 - processorBlue
 * <li> componentB - context1 - processorWhite
 * <li> componentA - context1 - processorWhite
 * <li> componentA - context1 - processorRed
 * </ul>
 * In other words, it provides all instances in the componentA plus
 * <ul>
 * <li> componentB - context1 - processorWhite
 * </ul>
 * that is the only one not part of <b>componentA</b> that has same context/name of one of the processors defined
 * from <b>componentA</b>.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public class ClientProcessorsByComponent implements ClientProcessors {

    private final ClientProcessors allEventProcessors;

    private final Predicate<ClientProcessor> existInComponent;

    /**
     * Creates an instance defined by the full list of all {@link ClientProcessor}s, component and context
     *
     * @param allEventProcessors all known {@link ClientProcessor}s
     * @param component          the component name of the client application
     * @param context            the context of the client application
     */
    ClientProcessorsByComponent(ClientProcessors allEventProcessors,
                                String component,
                                String context) {
        this(allEventProcessors, new ExistsInComponent(context, component, allEventProcessors));
    }

    /**
     * Creates an instance defined by the full list of all {@link ClientProcessor}s and a predicate to filter
     * the {@link ClientProcessor}s defined in the correct client application.
     *
     * @param allEventProcessors all known {@link ClientProcessor}s
     * @param existInComponent   the predicate to test if a {@link ClientProcessor} is defined in the client application
     */
    ClientProcessorsByComponent(ClientProcessors allEventProcessors, Predicate<ClientProcessor> existInComponent) {
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

        private final String context;

        /* Iterable of all Client Processors defined directly in the specified component*/
        private final Iterable<ClientProcessor> directComponentProcessors;

        ExistsInComponent(String context, String component, ClientProcessors allEventProcessors) {
            this(context, new ComponentItems<>(component, context, allEventProcessors));
        }

        ExistsInComponent(String context, Iterable<ClientProcessor> directComponentProcessors) {
            this.context = context;
            this.directComponentProcessors = directComponentProcessors;
        }

        @Override
        public boolean test(ClientProcessor processor) {
            return stream(directComponentProcessors.spliterator(), false)
                    .anyMatch(p -> new SameProcessor(context, p).test(processor));
        }
    }
}
