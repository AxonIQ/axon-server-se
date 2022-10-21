/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.commandprocesing.imp.CommandDispatcher;
import io.axoniq.axonserver.commandprocessing.spi.CapacityException;
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.commandprocessing.spi.NoHandlerFoundException;
import io.axoniq.axonserver.component.tags.ClientTagsCache;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.command.CommandServiceGrpc;
import io.axoniq.axonserver.transport.grpc.command.GrpcCommand;
import io.axoniq.axonserver.transport.grpc.command.GrpcMapper;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

@Service
public class CommandGrpcController extends CommandServiceGrpc.CommandServiceImplBase
        implements AxonServerClientService {

    private final Logger logger = LoggerFactory.getLogger(CommandGrpcController.class);
    private final ContextProvider contextProvider;
    private final AuthenticationProvider authenticationProvider;
    private final CommandRequestProcessor commandRequestProcessor;

    private final CommandDispatcher queuedCommandDispatcher;
    private final ClientTagsCache clientTagsCache;

    private final Map<UUID, CommandHandlerStream> commandHandlers = new ConcurrentHashMap<>();

    public CommandGrpcController(ContextProvider contextProvider, AuthenticationProvider authenticationProvider,
                                 CommandRequestProcessor commandRequestProcessor,
                                 CommandDispatcher queuedCommandDispatcher,
                                 ClientTagsCache clientTagsCache) {
        this.contextProvider = contextProvider;
        this.authenticationProvider = authenticationProvider;
        this.commandRequestProcessor = commandRequestProcessor;
        this.queuedCommandDispatcher = queuedCommandDispatcher;
        this.clientTagsCache = clientTagsCache;
    }

    @Override
    public StreamObserver<CommandProviderOutbound> openStream(StreamObserver<CommandProviderInbound> responseObserver) {
        return new StreamObserver<CommandProviderOutbound>() {
            private final UUID id = UUID.randomUUID();

            @Override
            public void onNext(CommandProviderOutbound commandProviderOutbound) {
                CommandHandlerStream commandHandlerStream =
                        getCommandHandlerStream(commandProviderOutbound,responseObserver);

                switch (commandProviderOutbound.getRequestCase()) {
                    case SUBSCRIBE:
                        commandHandlerStream.subscribe(commandProviderOutbound.getSubscribe())
                                            .subscribe(ignore -> {
                                                       },
                                                       e -> logger.error(
                                                               "Following error occurred while subscribing to command handler: ",
                                                               e),
                                                       () -> sendAck(commandProviderOutbound.getInstructionId()));
                        break;
                    case UNSUBSCRIBE:
                        commandHandlerStream.unsubscribe(commandProviderOutbound.getUnsubscribe())
                                            .subscribe(ignore -> {
                                                       },
                                                       e -> logger.error(
                                                               "Following error occurred while unsubscribing from command handler: ",
                                                               e),
                                                       () -> sendAck(commandProviderOutbound.getInstructionId()));
                        break;
                    case FLOW_CONTROL:
                        commandHandlerStream.flowControl(commandProviderOutbound.getFlowControl());
                        break;
                    case COMMAND_RESPONSE:
                        commandHandlerStream.commandResponse(commandProviderOutbound.getCommandResponse());
                        break;
                    case ACK:
                        // ignore
                        break;
                    case REQUEST_NOT_SET:
                        break;
                }
            }

            @Nonnull
            private CommandHandlerStream getCommandHandlerStream(CommandProviderOutbound commandProviderOutbound,
                                                                 StreamObserver<CommandProviderInbound> responseObserver) {
                return commandHandlers.computeIfAbsent(id, id -> new CommandHandlerStream(contextProvider.getContext(),
                                                                                          clientId(
                                                                                                  commandProviderOutbound),
                                                                                          responseObserver,
                                                                                          queuedCommandDispatcher,
                                                                                          commandRequestProcessor,
                                                                                          clientTagsCache));
            }

            private void sendAck(String instructionId) {
                responseObserver.onNext(CommandProviderInbound.newBuilder()
                                                              .setAck(InstructionAck.newBuilder()
                                                                                    .setSuccess(true)
                                                                                    .setInstructionId(instructionId)
                                                                                    .build())
                                                              .build());
            }

            private String clientId(CommandProviderOutbound request) {
                switch (request.getRequestCase()) {
                    case SUBSCRIBE:
                        return request.getSubscribe().getClientId();
                    case UNSUBSCRIBE:
                        return request.getUnsubscribe().getClientId();
                    case FLOW_CONTROL:
                        return request.getFlowControl().getClientId();
                    default:
                        return "Unknown handler id";
                }
            }

            @Override
            public void onError(Throwable throwable) {
                onCompleted();
            }

            @Override
            public void onCompleted() {
                CommandHandlerStream stream = commandHandlers.remove(id);
                if (stream != null) {
                    stream.cancel();
                }
            }
        };
    }

    //dispatches command from application to axon server
    @Override
    public void dispatch(Command request, StreamObserver<CommandResponse> responseObserver) {
        String context = contextProvider.getContext();
        Authentication authentication = authenticationProvider.get();
        commandRequestProcessor.dispatch(new GrpcCommand(request,
                                                         context,
                                                         new GrpcAuthentication(
                                                                 () -> authentication)))
                               .onErrorMap(NoHandlerFoundException.class,
                                           t -> new MessagingPlatformException(ErrorCode.NO_HANDLER_FOR_COMMAND,
                                                                               "No handler found for "
                                                                                       + request.getName()))
                               .onErrorMap(CapacityException.class,
                                           t -> new MessagingPlatformException(ErrorCode.TOO_MANY_REQUESTS,
                                                                               t.getMessage()))
                               .subscribe(
                                       result -> responseObserver.onNext(GrpcMapper.map(result)),
                                       error -> returnError(responseObserver, request, error),
                                       responseObserver::onCompleted);
    }

    private void returnError(StreamObserver<CommandResponse> responseObserver, Command request, Throwable e) {
        String errorCode = MessagingPlatformException.create(e).getErrorCode().getCode();
        CommandResponse commandResponse = CommandResponse.newBuilder()
                                                         .setRequestIdentifier(request.getMessageIdentifier())
                                                         .setMessageIdentifier(UUID.randomUUID().toString())
                                                         .setErrorCode(errorCode)
                                                         .setErrorMessage(ErrorMessage.newBuilder()
                                                                                      .setErrorCode(errorCode)
                                                                                      .setMessage(e.toString()))
                                                         .build();

        responseObserver.onNext(commandResponse);
        responseObserver.onCompleted();
    }
}
