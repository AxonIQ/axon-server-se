package io.axoniq.axonserver.refactoring.transport;

/**
 * @author Milan Savic
 */
@FunctionalInterface
public interface Mapper<From, To> {

    To map(From origin);
}
