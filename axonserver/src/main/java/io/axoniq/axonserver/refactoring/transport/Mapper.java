package io.axoniq.axonserver.refactoring.transport;

/**
 * @author Milan Savic
 */
//@FunctionalInterface
public interface Mapper<From, To> {

    To map(From origin);

    // TODO: 4/20/2021 unmap, really?
    From unmap(To origin);
}
