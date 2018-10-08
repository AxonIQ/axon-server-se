package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.platform.KeepNames;

import java.util.List;

/**
 * Author: marc
 */
@KeepNames
public class MessageJson {

    private final String location;
    private final String message;
    private final List<String> details;

    public MessageJson(ErrorMessage message) {
        this.message = message.getMessage();
        this.details = message.getDetailsList();
        this.location = message.getLocation();
    }

    public String getLocation() {
        return location;
    }

    public String getMessage() {
        return message;
    }

    public List<String> getDetails() {
        return details;
    }
}
