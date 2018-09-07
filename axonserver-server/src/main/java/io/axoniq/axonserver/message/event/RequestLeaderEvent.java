package io.axoniq.axonserver.message.event;

import io.axoniq.axonhub.internal.grpc.NodeContextInfo;

import java.util.function.Consumer;

/**
 * Author: marc
 */
public class RequestLeaderEvent  {

    private final NodeContextInfo request;
    private final Consumer<Boolean> callback;

    public RequestLeaderEvent(NodeContextInfo request, Consumer<Boolean> callback) {
        this.request = request;
        this.callback = callback;
    }

    public NodeContextInfo getRequest() {
        return request;
    }

    public Consumer<Boolean> getCallback() {
        return callback;
    }
}
