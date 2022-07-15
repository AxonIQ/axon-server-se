package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;

public interface Metadata {

    Flux<String> metadataKeys();

    Mono<Serializable> metadataValue(String metadataKey);
}
