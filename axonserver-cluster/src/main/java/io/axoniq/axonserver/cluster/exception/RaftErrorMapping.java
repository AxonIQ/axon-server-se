package io.axoniq.axonserver.cluster.exception;

import io.axoniq.axonserver.grpc.cluster.ErrorMessage;

import java.util.function.Function;

/**
 * Author: marc
 */
public class RaftErrorMapping implements Function<Throwable, ErrorMessage> {

    @Override
    public ErrorMessage apply(Throwable throwable) {
        return null;
    }
}
