package io.axoniq.axonserver;

import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;

import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public class DispatchEvents {
    @KeepNames
    public static class DispatchCommand {

        private final Command request;
        private final Consumer<CommandResponse> responseObserver;
        private final boolean proxied;
        private String context;

        public DispatchCommand(String context, Command request, Consumer<CommandResponse> responseObserver, boolean proxied) {
            this.context = context;
            this.request = request;
            this.responseObserver = responseObserver;
            this.proxied = proxied;
        }

        public Command getRequest() {
            return request;
        }

        public Consumer<CommandResponse> getResponseObserver() {
            return responseObserver;
        }

        public boolean isProxied() {
            return proxied;
        }

        public String getContext() {
            return context;
        }
    }

    @KeepNames
    public static class DispatchCommandResponse {

        private final CommandResponse commandResponse;

        private final boolean proxied;

        public DispatchCommandResponse(CommandResponse commandResponse, boolean proxied) {
            this.commandResponse = commandResponse;
            this.proxied = proxied;
        }

        public CommandResponse getCommandResponse() {
            return commandResponse;
        }

        public boolean isProxied() {
            return proxied;
        }
    }

    @KeepNames
    public static class DispatchQuery {

        private final String context;
        private final QueryRequest query;
        private final Consumer<QueryResponse> callback;
        private final Consumer<String> onCompleted;
        private final boolean proxied;

        public DispatchQuery(String context, QueryRequest query, Consumer<QueryResponse> callback, Consumer<String> onCompleted, boolean proxied) {
            this.context = context;
            this.query = query;
            this.callback = callback;
            this.onCompleted = onCompleted;
            this.proxied = proxied;
        }

        public QueryRequest getQuery() {
            return query;
        }

        public Consumer<QueryResponse> getCallback() {
            return callback;
        }

        public Consumer<String> getOnCompleted() {
            return onCompleted;
        }

        public boolean isProxied() {
            return proxied;
        }

        public String getContext() {
            return context;
        }
    }

    @KeepNames
    public static class DispatchQueryResponse {

        private final QueryResponse queryResponse;
        private final String client;
        private final boolean proxied;

        public DispatchQueryResponse(QueryResponse queryResponse, String client, boolean proxied) {

            this.queryResponse = queryResponse;
            this.client = client;
            this.proxied = proxied;
        }

        public QueryResponse getQueryResponse() {
            return queryResponse;
        }

        public String getClient() {
            return client;
        }

        public boolean isProxied() {
            return proxied;
        }
    }

    @KeepNames
    public static class DispatchQueryCompleted {

        private final String requestId;
        private final String client;
        private final boolean proxied;

        public DispatchQueryCompleted(String requestId, String client, boolean proxied) {
            this.requestId = requestId;
            this.client = client;
            this.proxied = proxied;
        }

        public String getRequestId() {
            return requestId;
        }

        public String getClient() {
            return client;
        }

        public boolean isProxied() {
            return proxied;
        }
    }
}
