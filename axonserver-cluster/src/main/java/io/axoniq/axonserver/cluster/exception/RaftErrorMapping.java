package io.axoniq.axonserver.cluster.exception;

import io.axoniq.axonserver.grpc.cluster.ErrorMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Mapping of cluster exceptions in {@link ErrorMessage}s
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class RaftErrorMapping implements Function<Throwable, ErrorMessage> {

    private final Map<Class, ErrorCode> codes = new HashMap<>();

    /**
     * Creates a {@link RaftErrorMapping} initializing the codes mapping
     */
    public RaftErrorMapping() {
        codes.put(ServerTooSlowException.class, ErrorCode.SERVER_TOO_SLOW);
        codes.put(UncommittedConfigException.class, ErrorCode.UNCOMMITTED_CONFIGURATION);
        codes.put(ReplicationTimeoutException.class, ErrorCode.REPLICATION_TIMEOUT);
    }

    /**
     * Produces a new {@link ErrorMessage} that represents the specified error
     *
     * @param throwable the error
     * @return the {@link ErrorMessage} that represents the error
     */
    @Override
    public ErrorMessage apply(Throwable throwable) {
        if( throwable == null) {
            return ErrorMessage.newBuilder().setCode(ErrorCode.CLUSTER_ERROR.code()).setMessage("Internal errror").build();
        }
        ErrorCode error = codes.getOrDefault(throwable.getClass(), ErrorCode.CLUSTER_ERROR);
        String message = throwable.getMessage();
        return ErrorMessage.newBuilder().setCode(error.code()).setMessage(message).build();
    }
}
