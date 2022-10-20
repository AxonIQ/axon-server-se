package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.Command;

public interface CommandException {

    Command command();

    Throwable exception();
}
