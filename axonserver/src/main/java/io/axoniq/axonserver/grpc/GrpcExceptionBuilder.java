package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Marc Gathier
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

    public static MessagingPlatformException parse(Throwable throwable) {
        if( throwable instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException)throwable;
            String errorCode = statusRuntimeException.getTrailers().get(GrpcMetadataKeys.ERROR_CODE_KEY);
            ErrorCode standardErrorCode = ErrorCode.find(errorCode);

            return new MessagingPlatformException(standardErrorCode, cleanupDescription(standardErrorCode, statusRuntimeException.getStatus().getDescription()));
        }
        return new MessagingPlatformException(ErrorCode.OTHER, throwable.getMessage());
    }

    // Trim AXONIQ error code from message
    private static String cleanupDescription(ErrorCode standardErrorCode, String description) {
        String stdPrefix = "[" + standardErrorCode.getCode() + "] ";
        if( description != null && description.startsWith(stdPrefix)) {
            return description.substring(stdPrefix.length());
        }
        return description;
    }
}
