package io.axoniq.axonserver.commandprocessing.spi.interceptor;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Interceptor;

public interface CommandResultDispatchedInterceptor extends Interceptor {

    void onCommandResultDispatched(CommandResult commandResult, Command command, CommandHandler commandHandler);
}
