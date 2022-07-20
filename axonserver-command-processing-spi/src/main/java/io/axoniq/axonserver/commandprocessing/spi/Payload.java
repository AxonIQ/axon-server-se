package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Flux;

public interface Payload {

    String type();

    String contentType();

    Flux<Byte> data();
}
