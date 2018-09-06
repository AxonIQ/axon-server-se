package io.axoniq.axonhub.exception;

import io.axoniq.axonhub.ErrorMessage;

/**
 * Author: marc
 */
public class ErrorMessageFactory {
    public static ErrorMessage build(String message) {
        return build(message, "AxonHub");
    }

    public static ErrorMessage build(String message, String location) {
        return ErrorMessage.newBuilder().setLocation(location).setMessage(message).build();
    }
}
