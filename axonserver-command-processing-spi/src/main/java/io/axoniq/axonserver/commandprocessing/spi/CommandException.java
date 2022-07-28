package io.axoniq.axonserver.commandprocessing.spi;

public interface CommandException {

    Command command();

    Throwable exception();
}
