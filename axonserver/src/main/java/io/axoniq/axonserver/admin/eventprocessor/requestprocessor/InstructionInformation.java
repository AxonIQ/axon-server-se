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
import io.axoniq.axonserver.admin.eventprocessor.api.Result;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import reactor.core.publisher.MonoSink;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.axonserver.admin.Result.ACK;
import static io.axoniq.axonserver.admin.Result.FAILURE;
import static io.axoniq.axonserver.admin.Result.SUCCESS;
import static java.lang.String.format;

/**
 * Default {@link Instruction} implementation.
 *
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class InstructionInformation implements Instruction {

    private final long timestamp;
    private final MonoSink<Result> completionHandler;
    private final String instructionId;
    private final String requestType;
    private final Set<String> targetClients;
    private final Set<String> waitingForClients;
    private final AtomicReference<Runnable> completionListener = new AtomicReference<>(() -> {
    });
    private final AtomicReference<io.axoniq.axonserver.admin.Result> resultRef = new AtomicReference<>(SUCCESS);

    /**
     * Constructs an instance based on the specified parameters.
     *
     * @param completionHandler a handler for the instruction completion
     * @param instructionId     the identifier of the instruction
     * @param requestType       the request type for logging purpose
     * @param targetClients     the set of client ids that should provide a result for the instruction
     */
    InstructionInformation(MonoSink<Result> completionHandler, String instructionId,
                           String requestType, Set<String> targetClients) {
        this(Instant.now().toEpochMilli(), completionHandler, instructionId, requestType, targetClients);
    }


    /**
     * Constructs an instance based on the specified parameters.
     *
     * @param timestamp         the time when the instruction has been sent
     * @param completionHandler a handler for the instruction completion
     * @param instructionId     the identifier of the instruction
     * @param requestType       the request type for logging purpose
     * @param targetClients     the set of client ids that should provide a result for the instruction
     */
    InstructionInformation(long timestamp, MonoSink<Result> completionHandler, String instructionId,
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
        if (FAILURE.equals(result.result())) {
            ErrorCode errorCode = ErrorCode.find(result.errorCode());
            completionHandler.error(new MessagingPlatformException(errorCode, result.errorMessage()));
            completionListener.get().run();
            return;
        }

        resultRef.updateAndGet(old -> old.and(result.result()));
        if (waitingForClients.isEmpty()) {
            completionHandler.success(map(resultRef.get()));
            completionListener.get().run();
        }
    }

    private Result map(io.axoniq.axonserver.admin.Result result) {
        return new Result() {
            @Override
            public boolean isSuccess() {
                return SUCCESS.equals(result);
            }

            @Override
            public boolean isAccepted() {
                return SUCCESS.equals(result) || ACK.equals(result);
            }
        };
    }

    @Override
    public void completeExceptionally(ErrorCode errorCode, String message) {
        completionHandler.error(new MessagingPlatformException(errorCode, message));
        completionListener.get().run();
    }

    @Override
    public boolean isWaitingFor(String clientId) {
        return waitingForClients.contains(clientId);
    }

    @Override
    public void onCompletion(@Nonnull Runnable listener) {
        this.completionListener.set(listener);
    }
}
