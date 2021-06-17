package io.axoniq.axonserver.grpc;

import java.util.Objects;

public class ClientContext {
    private final String clientId;
    private final String context;

    public ClientContext(String clientId, String context) {
        this.clientId = clientId;
        this.context = context;
    }

    public String clientId() {
        return clientId;
    }

    public String context() {
        return context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientContext that = (ClientContext) o;
        return Objects.equals(clientId, that.clientId) && Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, context);
    }

    @Override
    public String toString() {
        return clientId + "." + context;
    }
}
