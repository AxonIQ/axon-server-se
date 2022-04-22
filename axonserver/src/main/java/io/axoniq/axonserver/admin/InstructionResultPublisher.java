/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin;

import io.axoniq.axonserver.applicationevents.AxonServerEventPublisher;
import io.axoniq.axonserver.applicationevents.InstructionResultEvent;
import io.axoniq.axonserver.component.version.BackwardsCompatibleVersion;
import io.axoniq.axonserver.component.version.ClientVersionsCache;
import io.axoniq.axonserver.component.version.UnknownVersion;
import io.axoniq.axonserver.component.version.Version;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.InstructionResult;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.PlatformService.InstructionConsumer;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.RequestCase;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Arrays.asList;

/**
 * Component responsible to publish a Spring event when an {@link io.axoniq.axonserver.admin.InstructionResult} has been
 * received from a client.
 *
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @since 4.6.0
 */
@Component
public class InstructionResultPublisher {

    private static final String ERROR_MESSAGE_ACK_ONLY = "Instruction sent to client using Axon connector version prior to 4.6.0, instruction result not returned";
    private final AxonServerEventPublisher applicationEventPublisher;
    private final BiConsumer<RequestCase, InstructionConsumer> registerHandler;
    private final Function<ClientStreamIdentification, Version> versionSupplier;

    private final List<Version> supportedApiVersions = asList(
            new BackwardsCompatibleVersion("4.6"),
            new BackwardsCompatibleVersion("5")
    );

    /**
     * Constructs an instance based on the specified parameters.
     *
     * @param applicationEventPublisher the event publisher
     * @param platformService           the platform service used to register a handler for the specific request case
     */
    @Autowired
    public InstructionResultPublisher(AxonServerEventPublisher applicationEventPublisher,
                                      PlatformService platformService,
                                      ClientVersionsCache versionsCache) {
        this(applicationEventPublisher, platformService::onInboundInstruction, clientIdentification -> {
            String version = versionsCache.apply(clientIdentification);
            return (version == null || version.isEmpty()) ?
                    new UnknownVersion() : new BackwardsCompatibleVersion(version);
        });
    }

    /**
     * Constructs an instance based on the specified parameters.
     *
     * @param applicationEventPublisher the event publisher
     * @param registerHandler           a function used to register a handler for the specific request case
     */
    public InstructionResultPublisher(
            AxonServerEventPublisher applicationEventPublisher,
            BiConsumer<RequestCase, InstructionConsumer> registerHandler,
            Function<ClientStreamIdentification, Version> versionSupplier) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.registerHandler = registerHandler;
        this.versionSupplier = versionSupplier;
    }

    /**
     * Initialize the component registering itself as a listener for the instruction results.
     */
    @PostConstruct
    public void initialize() {
        registerHandler.accept(RequestCase.RESULT, this::publishResult);
        registerHandler.accept(RequestCase.ACK, this::publishResultOnAck);
    }

    private void publishResultOnAck(PlatformService.ClientComponent client,
                                    PlatformInboundInstruction platformInboundInstruction) {
        InstructionAck ack = platformInboundInstruction.getAck();
        if (!supportInstructionResult(versionSupplier.apply(new ClientStreamIdentification(client.getContext(),
                                                                                           client.getClientStreamId())))) {
            applicationEventPublisher.publishEvent(
                    new InstructionResultEvent(ack.getInstructionId(),
                                               client.getClientId(),
                                               ack.getSuccess() ? Result.ACK : Result.FAILURE,
                                               ack.hasError() ? ack.getError()
                                                                   .getErrorCode() : ErrorCode.INSTRUCTION_ACK_ONLY.getCode(),
                                               ack.hasError() ? ack.getError().getMessage() : ERROR_MESSAGE_ACK_ONLY));
        }
    }

    private void publishResult(PlatformService.ClientComponent client, PlatformInboundInstruction instruction) {
        InstructionResult result = instruction.getResult();
        applicationEventPublisher.publishEvent(new InstructionResultEvent(result.getInstructionId(),
                                                                          client.getClientId(),
                                                                          result.getSuccess() ? Result.SUCCESS : Result.FAILURE,
                                                                          result.hasError() ? result.getError()
                                                                                                    .getErrorCode() : null,
                                                                          result.hasError() ? result.getError()
                                                                                                    .getMessage() : null));
    }

    private boolean supportInstructionResult(Version clientVersion) {
        return clientVersion.greaterOrEqualThan(supportedApiVersions);
    }
}
