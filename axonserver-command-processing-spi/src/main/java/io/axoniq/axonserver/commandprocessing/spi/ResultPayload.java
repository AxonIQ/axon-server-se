package io.axoniq.axonserver.commandprocessing.spi;

public interface ResultPayload extends Payload {

    boolean error();
}
