package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;

public interface CommandHandlerUnsubscribedInterceptor extends Interceptor {

    void onCommandHandlerUnsubscribed(CommandHandler commandHandler);
}
