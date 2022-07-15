package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Flux;

public interface Payload {

    Flux<Byte> data();
}
