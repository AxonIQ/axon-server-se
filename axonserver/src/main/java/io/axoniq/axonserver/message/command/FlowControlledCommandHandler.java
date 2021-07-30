/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.CommandStream;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.heartbeat.ApplicationInactivityException;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marc Gathier
 */
public class FlowControlledCommandHandler extends CommandHandler {

    private static final Logger logger = LoggerFactory.getLogger(FlowControlledCommandHandler.class);
    private final Map<String, MonoSink<SerializedCommandResponse>> commandCache = new ConcurrentHashMap<String, reactor.core.publisher.MonoSink<SerializedCommandResponse>>();
    private final CommandStream listener;
    private final String queueName;

    public FlowControlledCommandHandler(ClientStreamIdentification clientStreamIdentification, String clientId,
                                        String componentName,
                                        CommandStream listener) {
        super(clientStreamIdentification, clientId, componentName);
        this.queueName = clientStreamIdentification.toString();
        this.listener = listener;
    }

    @Override
    public Mono<SerializedCommandResponse> dispatch(SerializedCommand request) {
        return Mono.create(sink -> {
            WrappedCommand wrappedCommand = new WrappedCommand(clientStreamIdentification,
                                                               getClientId(), request);

            commandCache.put(request.getMessageIdentifier(), sink);
            listener.emitNext(wrappedCommand, (signalType, emitResult) -> {
                if( emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED)) {
                    return true;
                }
                logger.warn("Failed to dispatch. Signal {}, result {}", signalType, emitResult);
                commandResponse(cannotEmit(request.getMessageIdentifier()));
                return false;
            });
        });
    }

    private SerializedCommandResponse cannotEmit(String messageIdentifier) {
        return error(messageIdentifier, ErrorCode.COMMAND_DISPATCH_ERROR);
    }

    public void commandResponse(SerializedCommandResponse commandResponse) {
        MonoSink<SerializedCommandResponse> sink = commandCache.remove(commandResponse.getRequestIdentifier());
        if( sink != null) {
            sink.success(commandResponse);
        }
    }

    @Override
    public String getMessagingServerName() {
        return null;
    }

    public void close() {
        List<WrappedCommand> queued = listener.cancel();
        cancelPending(queued);
    }

    private void cancelPending(List<WrappedCommand> queued) {
        queued.forEach(command -> commandResponse(error(command.command().getMessageIdentifier(), ErrorCode.CONNECTION_TO_HANDLER_LOST)));
        commandCache.forEach((request, sink) -> sink.success(error(request, ErrorCode.CONNECTION_TO_HANDLER_LOST)));
    }

    private SerializedCommandResponse error(String messageIdentifier, ErrorCode connectionToHandlerLost) {
        return new SerializedCommandResponse(CommandResponse.newBuilder()
                                                            .setMessageIdentifier(UUID.randomUUID().toString())
                                                            .setRequestIdentifier(messageIdentifier)
                                                            .setErrorCode(connectionToHandlerLost.getCode())
                                                            .setErrorMessage(ErrorMessage.newBuilder()
                                                                                         .setErrorCode(connectionToHandlerLost.getCode())
                                                                                         .setMessage("TODO")
                                                                                         .build())
                                                            .build());
    }

    public void cancelAndCompleteStreamExceptionally(ApplicationInactivityException exception) {
        List<WrappedCommand> queued = listener.tryEmitError(exception);
        cancelPending(queued);
    }

    public String queue() {
        return queueName;
    }

    public int waiting() {
        return listener.waiting();
    }

    public long permits() {
        return listener.permits();
    }
}
