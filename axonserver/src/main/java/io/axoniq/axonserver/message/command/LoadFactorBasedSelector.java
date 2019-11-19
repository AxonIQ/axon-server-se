package io.axoniq.axonserver.message.command;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * Selects the best handler to dispatch the message to, distributing routing keys on the base of load factor.
 *
 * @author Sara Pellegrini
 * @since 4.3
 */
public class LoadFactorBasedSelector<Handler> implements RoutingSelector<Handler> {

    private final Collection<Handler> handlers = new CopyOnWriteArrayList<>();

    private final Function<Handler, Integer> loadFactorSolver;

    public LoadFactorBasedSelector(Function<Handler, Integer> loadFactorSolver) {
        this.loadFactorSolver = loadFactorSolver;
    }

    /**
     * Selects the best handler on the base of load factor
     *
     * @param routingKey the routing key for the message
     * @return the best handler to route the message to.
     */
    @Override
    public Optional<Handler> selectHandler(String routingKey) {
        int total = total();
        if (total == 0) {
            return Optional.empty();
        }
        int i = routingKey.hashCode();
        int module = i % total * Integer.signum(i);
        int min = 0;
        for (Handler handler : handlers) {
            int max = min + loadFactorSolver.apply(handler);
            if (min <= module && max > module) {
                return Optional.of(handler);
            }
            min = max;
        }
        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     *
     * @param handler the message handler identifier
     */
    @Override
    public void register(Handler handler) {
        if (!handlers.contains(handler)) {
            handlers.add(handler);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param handler the message handler identifier
     */
    @Override
    public void unregister(Handler handler) {
        handlers.remove(handler);
    }


    private int total() {
        return handlers.stream()
                       .map(loadFactorSolver)
                       .reduce(Integer::sum)
                       .orElse(0);
    }
}
