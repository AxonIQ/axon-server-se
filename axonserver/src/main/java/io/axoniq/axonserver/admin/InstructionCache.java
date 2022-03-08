/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin;

import io.axoniq.axonserver.ActiveRequestsCache;
import io.axoniq.axonserver.CancelOnTimeout;
import io.axoniq.axonserver.LimitedBuffer;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.util.ConstraintCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static java.lang.String.format;

/**
 * Component used to cache active instructions.
 *
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @since 4.6.0
 */
@Component
public class InstructionCache extends ActiveRequestsCache<Instruction> {

    private static final String REQUEST_TYPE = "Instruction";
    private static final String FULL_BUFFER_MESSAGE = "Instruction handlers might be slow.";

    private final CancelStrategy<Instruction> onTimeout;

    /**
     * Constructs an instance based on the specified parameters.
     *
     * @param capacity the max number of instruction that can be cached
     * @param timeout  the timeout after which to cancel the uncompleted instruction
     */
    @Autowired
    public InstructionCache(
            @Value("${axoniq.axonserver.instruction-cache-capacity:1000}") long capacity,
            @Value("${axoniq.axonserver.instruction-cache-timeout:10000}") long timeout) {
        this(new LimitedBuffer<>(REQUEST_TYPE, FULL_BUFFER_MESSAGE, capacity), timeout);
    }

    /**
     * Constructs an instance based on the specified parameters.
     *
     * @param buffer  the cache buffer to collect active instructions
     * @param timeout the timeout after which to cancel the uncompleted instruction
     */
    public InstructionCache(ConstraintCache<String, Instruction> buffer, long timeout) {
        super(buffer);
        this.onTimeout = new CancelOnTimeout<>(REQUEST_TYPE,
                                               timeout,
                                               Instruction::description,
                                               Instruction::timestamp,
                                               this::completeOnTimeout);
    }

    private void completeOnTimeout(Instruction instruction) {
        String message = format("Timeout on instruction %s", instruction.description());
        instruction.completeExceptionally(ErrorCode.INSTRUCTION_TIMEOUT, message);
    }

    /**
     * Schedules the timeout check.
     */
    @Scheduled(fixedDelayString = "${axoniq.axonserver.cache-close-rate:5000}")
    public void checkTimeout() {
        this.cancel(onTimeout);
    }

    /**
     * Cancels instructions when the handler disconnects.
     *
     * @param applicationDisconnected the client disconnected event
     */
    @EventListener
    public void on(ApplicationDisconnected applicationDisconnected) {
        String clientId = applicationDisconnected.getClientId();
        CancelStrategy<Instruction> onApplicationDisconnected = new CancelOnHandlerDisconnected(clientId);
        this.cancel(onApplicationDisconnected);
    }
}
