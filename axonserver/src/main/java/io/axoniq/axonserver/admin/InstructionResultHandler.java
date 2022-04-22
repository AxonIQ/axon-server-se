/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin;

import io.axoniq.axonserver.applicationevents.InstructionResultEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Component listening and handling the instruction results.
 *
 * @author Sara Pellegrini
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class InstructionResultHandler {

    private final InstructionCache instructionCache;

    /**
     * Constructs an instance based on the specified instruction cache.
     *
     * @param instructionCache the active instruction cache.
     */
    public InstructionResultHandler(InstructionCache instructionCache) {
        this.instructionCache = instructionCache;
    }

    /**
     * Handles the instruction result.
     *
     * @param event the instruction result event.
     */
    @EventListener
    public void on(InstructionResultEvent event) {
        Instruction instruction = instructionCache.get(event.instructionId());
        if (instruction != null) {
            instruction.on(new InstructionResult() {
                @Override
                public String clientId() {
                    return event.clientId();
                }

                @Override
                public Result result() {
                    return event.result();
                }

                @Override
                public String errorCode() {
                    return event.errorCode();
                }

                @Override
                public String errorMessage() {
                    return event.errorMessage();
                }
            });
        }
    }
}
