package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;

/**
 * @author Marc Gathier
 */
@KeepNames
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
