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
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.component.tags.ClientTagsCache;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Represents command stream from Axon Server to application that handles commands
 *
 * @author Marc Gathier
 * @author Stefan Dragisic
 */
class CommandHandlerStream {

    private final String streamId = UUID.randomUUID().toString();
    private final String context;
    private final String clientId;
    private final StreamObserver<CommandProviderInbound> streamToHandler;

    private final Map<String, String> registrations = new ConcurrentHashMap<>();
    private final Map<String, MonoSink<CommandResponse>> activeCommands = new ConcurrentHashMap<>();
    private final CommandDispatcher queuedCommandDispatcher;
    private final CommandRequestProcessor commandRequestProcessor;
    private final ClientTagsCache clientTagsCache;

    public CommandHandlerStream(String context, String clientId,
                                StreamObserver<CommandProviderInbound> streamToHandler,
                                CommandDispatcher queuedCommandDispatcher,
                                CommandRequestProcessor commandRequestProcessor,
                                ClientTagsCache clientTagsCache) {
        this.context = context;
        this.clientId = clientId;
        this.streamToHandler = streamToHandler;
        this.queuedCommandDispatcher = queuedCommandDispatcher;
        this.commandRequestProcessor = commandRequestProcessor;
        this.clientTagsCache = clientTagsCache;
    }

    public Mono<Void> subscribe(CommandSubscription subscribe) {
        GrpcCommandHandlerSubscription handler = new GrpcCommandHandlerSubscription(subscribe,
                                                                                    clientId,
                                                                                    streamId,
                                                                                    context,
                                                                                    () -> tags(clientId,
                                                                                               context),
                                                                                    this::dispatch
        );
        return commandRequestProcessor.register(handler)
                .doOnSuccess(registration -> registrations.put(subscribe.getCommand(),
                        handler.commandHandler().id()));
    }

    public Mono<Void> unsubscribe(CommandSubscription unsubscribe) {
        return Mono.defer(()-> {
            String handlerId = registrations.remove(unsubscribe.getCommand());
            if (handlerId != null) {
                return commandRequestProcessor.unregister(handlerId);
            }
            return Mono.empty();
        });
    }

    public void flowControl(FlowControl flowControl) {
        queuedCommandDispatcher.request(clientId, flowControl.getPermits());
    }

    public void commandResponse(CommandResponse commandResponse) {
        MonoSink<CommandResponse> sink = activeCommands.remove(commandResponse.getRequestIdentifier());
        if (sink != null) {
            sink.success(commandResponse);
        }
    }

    public void cancel() {
        registrations.forEach((unused, handlerId) -> commandRequestProcessor.unregister(handlerId).block());
        // cancelling active commands should be done on unsubscribe in the command request processor
        activeCommands.forEach((requestId, sink) -> sink.error(new MessagingPlatformException(ErrorCode.CONNECTION_TO_HANDLER_LOST,
                clientId
                        + ": connection lost")));
    }

    //dispatches command from axon server to command handler (via command handler stream)
    private Mono<CommandResponse> dispatch(Command command) {
        return Mono.create(sink -> {
            activeCommands.put(command.getMessageIdentifier(), sink);
            streamToHandler.onNext(CommandProviderInbound.newBuilder()
                    .setCommand(command)
                    .build());
        });
    }

    private Map<String, String> tags(String clientId, String context) {
        return clientTagsCache.get(clientId, context);
    }
}
