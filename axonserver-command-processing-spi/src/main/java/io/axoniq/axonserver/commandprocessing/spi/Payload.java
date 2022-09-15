package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Flux;

import java.io.Serializable;

public interface Payload extends Serializable {

    String type();

    String contentType();

    Flux<Byte> data();
}
