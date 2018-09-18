package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author: marc
 */
public class GrpcExceptionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(GrpcExceptionBuilder.class);
    public static StatusRuntimeException build(ErrorCode errorCode, String description) {
        Metadata metadata = new Metadata();
        metadata.put(GrpcMetadataKeys.ERROR_CODE_KEY, errorCode.getCode());
        return errorCode.getGrpcCode().withDescription(description).asRuntimeException(metadata);
    }

    public static StatusRuntimeException build(Throwable throwable) {
        if( throwable instanceof MessagingPlatformException) {
            MessagingPlatformException eventStoreException = (MessagingPlatformException) throwable;
            return build(eventStoreException.getErrorCode(), eventStoreException.getMessage());
        }
        logger.debug("Internal Server Error found", throwable);
        return build(ErrorCode.OTHER, throwable.getMessage());
    }
}
