package io.axoniq.axonserver.message.command;

import java.util.Optional;
import java.util.Set;

/**
 * Selects the best handler to dispatch the message to, chosen among registered handlers.
 *
 * @param <Handler> the type of registered handlers
 * @author Sara Pellegrini
 * @since 4.3
 */
public interface RoutingSelector<Handler> {

    /**
     * Selects the best handler to dispatch the message to.
     *
     * @param routingKey the routing key for the message
     * @param candidates list of candidates to choose from, if null no further filtering on candidates is done
     * @return optional value of best handler to dispatch the message to.
     */
    Optional<Handler> selectHandler(String routingKey,
                                    Set<Handler> candidates);

    /**
     * Selects the best handler to dispatch the message to. Allows all candidates to be considered.
     *
     * @param routingKey the routing key for the message
     * @return optional value of best handler to dispatch the message to.
     */
    default Optional<Handler> selectHandler(String routingKey) {
        return selectHandler(routingKey, null);
    }

    /**
     * Registers a message handler.
     *
     * @param handler the message handler identifier
     */
    void register(Handler handler);

    /**
     * Unregisters a message handler.
     *
     * @param handler the message handler identifier
     */
    void unregister(Handler handler);
}
