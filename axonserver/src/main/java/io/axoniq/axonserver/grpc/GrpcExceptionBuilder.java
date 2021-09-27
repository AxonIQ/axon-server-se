/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.plugin.PostCommitHookException;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

/**
 * Creates a gRPC {@link StatusRuntimeException} from an exception. If the Exception is a {@link
 * MessagingPlatformException} it adds the error code as metadata.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class GrpcExceptionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(GrpcExceptionBuilder.class);
    public static StatusRuntimeException build(ErrorCode errorCode, String description) {
        Metadata metadata = new Metadata();
        metadata.put(GrpcMetadataKeys.ERROR_CODE_KEY, errorCode.getCode());
        return errorCode.getGrpcCode().withDescription(description).asRuntimeException(metadata);
    }

    public static StatusRuntimeException build(Throwable throwable) {
        if (throwable instanceof CompletionException) {
            throwable = throwable.getCause();
        }
        if (throwable instanceof MessagingPlatformException) {
            MessagingPlatformException eventStoreException = (MessagingPlatformException) throwable;
            return build(eventStoreException.getErrorCode(), eventStoreException.getMessage());
        }
        if (throwable instanceof PostCommitHookException) {
            return build(ErrorCode.POST_COMMIT_HOOK_EXCEPTION, throwable.getMessage());
        }
        logger.debug("Internal Server Error found", throwable);
        return build(ErrorCode.OTHER, throwable.getMessage());
    }

    public static MessagingPlatformException parse(Throwable throwable) {
        ErrorCode standardErrorCode = ErrorCode.OTHER;
        if( throwable instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException) throwable;
            if (statusRuntimeException.getTrailers() != null) {
                String errorCode = statusRuntimeException.getTrailers().get(GrpcMetadataKeys.ERROR_CODE_KEY);
                standardErrorCode = ErrorCode.find(errorCode);
            }

            return new MessagingPlatformException(standardErrorCode,
                                                  cleanupDescription(standardErrorCode,
                                                                     statusRuntimeException.getStatus()
                                                                                           .getDescription(),
                                                                     throwable),
                                                  throwable);
        }
        return new MessagingPlatformException(standardErrorCode, createMessage(throwable));
    }

    private static String createMessage(Throwable throwable) {
        List<String> lines = new ArrayList<>();
        while( throwable != null) {
            lines.add( throwable.getMessage());
            throwable = throwable.getCause();
        }

        return String.join(" - ", lines);
    }

    // Trim AXONIQ error code from message
    private static String cleanupDescription(ErrorCode standardErrorCode, String description, Throwable throwable) {
        String stdPrefix = "[" + standardErrorCode.getCode() + "] ";
        if( description != null && description.startsWith(stdPrefix)) {
            return description.substring(stdPrefix.length());
        }
        return createMessage(throwable);
    }
}
