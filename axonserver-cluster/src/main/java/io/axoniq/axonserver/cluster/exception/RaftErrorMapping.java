package io.axoniq.axonserver.cluster.exception;

import io.axoniq.axonserver.grpc.cluster.ErrorMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class RaftErrorMapping implements Function<Throwable, ErrorMessage> {

    private final Map<Class, String> codes = new HashMap<Class, String>() {{
        this.put(ServerTooSlowException.class, "AXONIQ-10001");
        this.put(UncommittedConfigException.class, "AXONIQ-10002");
    }};

    @Override
    public ErrorMessage apply(Throwable throwable) {
        String code = codes.getOrDefault(throwable.getClass(), "AXONIQ-10000");
        String message = throwable.getMessage();
        return ErrorMessage.newBuilder().setCode(code).setMessage(message).build();
    }
}
