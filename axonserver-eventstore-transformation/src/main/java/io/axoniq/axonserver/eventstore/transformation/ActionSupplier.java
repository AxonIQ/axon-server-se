package io.axoniq.axonserver.eventstore.transformation;

import reactor.core.publisher.Mono;

import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface ActionSupplier extends Supplier<Mono<Void>> {

}
