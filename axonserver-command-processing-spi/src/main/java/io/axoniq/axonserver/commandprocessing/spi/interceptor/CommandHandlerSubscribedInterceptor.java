package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;

public interface CommandHandlerSubscribedInterceptor extends Interceptor {

    void onCommandHandlerSubscribed(CommandHandler commandHandler);
}
