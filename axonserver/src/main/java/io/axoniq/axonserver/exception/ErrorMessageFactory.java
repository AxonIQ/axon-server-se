package io.axoniq.axonserver.exception;

import io.axoniq.axonserver.grpc.ErrorMessage;

/**
 * Creates error messages.
 *
 * @author Marc Gathier
 */
public class ErrorMessageFactory {
    public static ErrorMessage build(String message) {
        return build(message, "AxonServer");
    }

    public static ErrorMessage build(String message, String location) {
        return ErrorMessage.newBuilder().setLocation(location).setMessage(message).build();
    }
}
