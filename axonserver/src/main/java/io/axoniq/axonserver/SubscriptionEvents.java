package io.axoniq.axonserver;

import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.DirectCommandHandler;
import io.axoniq.axonserver.message.query.DirectQueryHandler;
import io.axoniq.axonserver.message.query.QueryHandler;

/**
 * @author Marc Gathier
 */
public class SubscriptionEvents {
    @KeepNames
    public abstract static class SubscriptionBaseEvent {
        private final String context;
        private final boolean isProxied;

        SubscriptionBaseEvent(String context, boolean isProxied) {
            this.context = context;
            this.isProxied = isProxied;
        }

        public String getContext() {
            return context;
        }

        public boolean isProxied() {
            return isProxied;
        }
    }
    @KeepNames
    public static class UnsubscribeCommand extends SubscriptionBaseEvent {

        private final CommandSubscription request;

        public UnsubscribeCommand(String context, CommandSubscription request, boolean isProxied) {
            super(context, isProxied);
            this.request = request;
        }

        public CommandSubscription getRequest() {
            return request;
        }

    }

    @KeepNames
    public static class UnsubscribeQuery extends SubscriptionBaseEvent {
        private final QuerySubscription unsubscribe;

        public UnsubscribeQuery(String context, QuerySubscription unsubscribe, boolean isProxied) {
            super(context, isProxied);
            this.unsubscribe = unsubscribe;
        }

        public QuerySubscription getUnsubscribe() {
            return unsubscribe;
        }

    }
    @KeepNames
    public static class SubscribeQuery extends SubscriptionBaseEvent {

        private final QuerySubscription subscription;
        private final QueryHandler queryHandler;

        public SubscribeQuery(String context, QuerySubscription subscription, QueryHandler queryHandler) {
            super(context, !(queryHandler instanceof DirectQueryHandler));
            this.subscription = subscription;
            this.queryHandler = queryHandler;
        }

        public QuerySubscription getSubscription() {
            return subscription;
        }

        public QueryHandler getQueryHandler() {
            return queryHandler;
        }

    }

    @KeepNames
    public static class SubscribeCommand extends SubscriptionBaseEvent {

        private final CommandSubscription request;
        private final CommandHandler handler;

        public SubscribeCommand(String context, CommandSubscription request, CommandHandler handler) {
            super(context, !(handler instanceof DirectCommandHandler));
            this.request = request;
            this.handler = handler;
        }

        public CommandSubscription getRequest() {
            return request;
        }

        public CommandHandler getHandler() {
            return handler;
        }

    }
}
