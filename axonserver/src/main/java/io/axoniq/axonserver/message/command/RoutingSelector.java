package io.axoniq.axonserver.message.command;

import java.util.Optional;

/**
 * Selects the best handler to dispatch the message to, chosen among registered handlers.
 *
 * @author Sara Pellegrini
 * @since 4.3
 */
public interface RoutingSelector<Handler> {

    /**
     * Selects the best handler to dispatch the message to.
     *
     * @param routingKey the routing key for the message
     * @return optional value of best handler to dispatch the message to.
     */
    Optional<Handler> selectHandler(String routingKey);

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
