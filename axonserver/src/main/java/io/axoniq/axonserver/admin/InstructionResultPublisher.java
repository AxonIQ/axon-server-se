/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin;

import io.axoniq.axonserver.applicationevents.AxonServerEventPublisher;
import io.axoniq.axonserver.applicationevents.InstructionResultEvent;
import io.axoniq.axonserver.grpc.InstructionResult;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.PlatformService.InstructionConsumer;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.RequestCase;
import org.springframework.stereotype.Component;

import java.util.function.BiConsumer;
import javax.annotation.PostConstruct;

/**
 * @author Sara Pellegrini
 * @since 4.6.0
 */
@Component
public class InstructionResultPublisher {

    private final AxonServerEventPublisher applicationEventPublisher;
    private final BiConsumer<RequestCase, InstructionConsumer> registerHandler;

    public InstructionResultPublisher(AxonServerEventPublisher applicationEventPublisher,
                                      PlatformService platformService) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.registerHandler = platformService::onInboundInstruction;
    }

    @PostConstruct
    public void initialize() {
        registerHandler.accept(RequestCase.RESULT, this::publishResult);
    }

    private void publishResult(PlatformService.ClientComponent client, PlatformInboundInstruction instruction) {
        InstructionResult result = instruction.getResult();
        applicationEventPublisher.publishEvent(new InstructionResultEvent(result.getInstructionId(),
                                                                          client.getClientId(),
                                                                          result.getSuccess(),
                                                                          result.hasError() ? result.getError()
                                                                                                    .getErrorCode() : null,
                                                                          result.hasError() ? result.getError()
                                                                                                    .getMessage() : null));
    }
}
