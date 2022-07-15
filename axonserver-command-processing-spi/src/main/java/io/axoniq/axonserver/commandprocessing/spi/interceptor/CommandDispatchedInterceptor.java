package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;

public interface CommandDispatchedInterceptor extends Interceptor {

    void onCommandDispatched(Command command, CommandHandler handler);
}
