package io.axoniq.axonserver.rest.json;

/**
 * Author: marc
 */
public class RestResponse {
    private final boolean success;
    private final String message;

    public RestResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

}
