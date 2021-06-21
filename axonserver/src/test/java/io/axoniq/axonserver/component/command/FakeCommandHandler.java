package io.axoniq.axonserver.component.command;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.command.CommandHandler;

import java.util.concurrent.atomic.AtomicInteger;

public class FakeCommandHandler extends CommandHandler {
    private final AtomicInteger requestCount = new AtomicInteger();

    public FakeCommandHandler(ClientStreamIdentification clientStreamIdentification, String client, String componentA) {
        super(clientStreamIdentification, client,componentA);
    }

    @Override
    public void dispatch(SerializedCommand request) {
        requestCount.incrementAndGet();
    }

    @Override
    public String getMessagingServerName() {
        return null;
    }

    public int requests() {
        return requestCount.get();
    }
}
