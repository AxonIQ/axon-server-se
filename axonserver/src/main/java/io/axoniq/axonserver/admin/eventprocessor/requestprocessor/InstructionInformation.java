/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import io.axoniq.axonserver.admin.Instruction;
import io.axoniq.axonserver.admin.InstructionResult;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import reactor.core.publisher.MonoSink;

import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

import static java.lang.String.format;

/**
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class InstructionInformation implements Instruction {

    private final long timestamp;
    private final MonoSink<Void> completionHandler;
    private final String instructionId;
    private final String requestType;
    private final Set<String> targetClients;
    private final Set<String> waitingForClients;
    private final AtomicReference<Runnable> completionListener = new AtomicReference<>(() -> {
    });

    public InstructionInformation(MonoSink<Void> completionHandler, String instructionId,
                                  String requestType, Set<String> targetClients) {
        this(Instant.now().toEpochMilli(), completionHandler, instructionId, requestType, targetClients);
    }

    public InstructionInformation(long timestamp, MonoSink<Void> completionHandler, String instructionId,
                                  String requestType, Set<String> targetClients) {
        this.timestamp = timestamp;
        this.completionHandler = completionHandler;
        this.instructionId = instructionId;
        this.requestType = requestType;
        this.targetClients = Collections.unmodifiableSet(targetClients);
        this.waitingForClients = new CopyOnWriteArraySet<>(targetClients);
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public String description() {
        return format("%s instruction request [%s] waiting for response from %s",
                      requestType,
                      instructionId,
                      targetClients);
    }

    @Override
    public void on(InstructionResult result) {
        String clientId = result.clientId();
        if (!waitingForClients.remove(clientId)) {
            return;
        }
        if (!result.success()) {
            ErrorCode errorCode = ErrorCode.find(result.errorCode());
            completionHandler.error(new MessagingPlatformException(errorCode, result.errorMessage()));
            completionListener.get().run();
            return;
        }
        if (waitingForClients.isEmpty()) {
            completionHandler.success();
            completionListener.get().run();
        }
    }

    @Override
    public void completeExceptionally(ErrorCode errorCode, String message) {
        completionHandler.error(new MessagingPlatformException(errorCode, message));
        completionListener.get().run();
    }

    @Override
    public boolean isWaitingFor(String clientStreamId) {
        return waitingForClients.contains(clientStreamId);
    }

    @Override
    public void onCompletion(@Nonnull Runnable listener) {
        this.completionListener.set(listener);
    }
}
