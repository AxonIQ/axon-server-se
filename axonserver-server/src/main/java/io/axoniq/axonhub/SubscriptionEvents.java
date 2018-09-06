package io.axoniq.axonhub;

import io.axoniq.axonhub.grpc.internal.ProxyCommandHandler;
import io.axoniq.axonhub.grpc.internal.ProxyQueryHandler;
import io.axoniq.axonhub.message.command.CommandHandler;
import io.axoniq.axonhub.message.query.QueryHandler;

/**
 * Author: marc
 */
public class SubscriptionEvents {
    public abstract static class SubscriptionBaseEvent {

    }
    @KeepNames
    public static class UnsubscribeCommand extends SubscriptionBaseEvent {

        private final String context;
        private final CommandSubscription request;
        private final boolean isProxied;

        public UnsubscribeCommand(String context, CommandSubscription request, boolean isProxied) {
            this.context = context;
            this.request = request;
            this.isProxied = isProxied;
        }

        public String getContext() {
            return context;
        }

        public CommandSubscription getRequest() {
            return request;
        }

        public boolean isProxied() {
            return isProxied;
        }
    }

    @KeepNames
    public static class UnsubscribeQuery extends SubscriptionBaseEvent {
        private final String context;
        private final QuerySubscription unsubscribe;
        private final boolean isProxied;

        public UnsubscribeQuery(String context, QuerySubscription unsubscribe, boolean isProxied) {
            this.context = context;
            this.unsubscribe = unsubscribe;
            this.isProxied = isProxied;
        }

        public String getContext() {
            return context;
        }

        public QuerySubscription getUnsubscribe() {
            return unsubscribe;
        }

        public boolean isProxied() {
            return isProxied;
        }
    }
    @KeepNames
    public static class SubscribeQuery extends SubscriptionBaseEvent {

        private final String context;
        private final QuerySubscription subscription;
        private final QueryHandler queryHandler;

        public SubscribeQuery(String context, QuerySubscription subscription, QueryHandler queryHandler) {
            this.context = context;
            this.subscription = subscription;
            this.queryHandler = queryHandler;
        }

        public String getContext() {
            return context;
        }

        public QuerySubscription getSubscription() {
            return subscription;
        }

        public QueryHandler getQueryHandler() {
            return queryHandler;
        }

        public boolean isProxied() {
            return queryHandler instanceof ProxyQueryHandler;
        }
    }

    @KeepNames
    public static class SubscribeCommand extends SubscriptionBaseEvent {

        private final String context;
        private final CommandSubscription request;
        private final CommandHandler handler;

        public SubscribeCommand(String context, CommandSubscription request, CommandHandler handler) {
            this.context = context;
            this.request = request;
            this.handler = handler;
        }

        public String getContext() {
            return context;
        }

        public CommandSubscription getRequest() {
            return request;
        }

        public CommandHandler getHandler() {
            return handler;
        }

        public boolean isProxied() {
            return handler instanceof ProxyCommandHandler;
        }
    }
}
