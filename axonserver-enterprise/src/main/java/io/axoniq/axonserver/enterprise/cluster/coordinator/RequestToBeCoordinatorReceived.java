package io.axoniq.axonserver.enterprise.cluster.coordinator;

import io.axoniq.axonserver.grpc.internal.NodeContext;

import java.util.function.Consumer;

/**
 * Created by Sara Pellegrini on 23/08/2018.
 * sara.pellegrini@gmail.com
 */
public class RequestToBeCoordinatorReceived {

    private final NodeContext request;
    private final Consumer<Boolean> callback;

    public RequestToBeCoordinatorReceived(NodeContext request, Consumer<Boolean> callback) {
        this.request = request;
        this.callback = callback;
    }

    public NodeContext request() {
        return request;
    }

    public Consumer<Boolean> callback() {
        return callback;
    }
}
