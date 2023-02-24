package io.axoniq.axonserver.eventstore.transformation;

/**
 * When this exception is thrown during the execution of a replication log entry consumer,
 * the apply operation completes with error, but it will not be retried.
 * Therefore, all implementations of this exception must be thrown in deterministic situations.
 * This is important to guarantee state replication consistency.
 *
 * @author Milan Savic
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface NonTransientTransformationException {

}
