package io.axoniq.axonserver.cluster.exception;

import io.axoniq.axonserver.grpc.cluster.ErrorMessage;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static io.axoniq.axonserver.cluster.exception.ErrorCode.CLUSTER_ERROR;

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
        codes.put(ReplicationInterruptedException.class, ErrorCode.REPLICATION_INTERRUPTED);
    }

    /**
     * Produces a new {@link ErrorMessage} that represents the specified error
     *
     * @param t the error
     * @return the {@link ErrorMessage} that represents the error
     */
    @Override
    public ErrorMessage apply(Throwable t) {
        if (t == null) {
            return ErrorMessage.newBuilder().setCode(CLUSTER_ERROR.code()).setMessage("Internal error")
                               .build();
        }
        Throwable throwable = t;
        while (throwable instanceof CompletionException) {
            throwable = throwable.getCause();
        }
        ErrorCode error = codes.getOrDefault(throwable.getClass(), CLUSTER_ERROR);
        String message = throwable.getMessage();
        if (message == null) {
            StringWriter errors = new StringWriter();
            throwable.printStackTrace(new PrintWriter(errors));
            message = errors.toString();
        }
        return ErrorMessage.newBuilder().setCode(error.code()).setMessage(message).build();
    }
}
