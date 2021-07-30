package io.axoniq.axonserver.component.command;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.command.CommandHandler;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class FakeCommandHandler extends CommandHandler {
    private final AtomicInteger requestCount = new AtomicInteger();
    private volatile CompletableFuture<SerializedCommandResponse> futureResponse;

    public FakeCommandHandler(ClientStreamIdentification clientStreamIdentification, String client, String componentA) {
        super(clientStreamIdentification, client,componentA);
    }

    @Override
    public Mono<SerializedCommandResponse> dispatch(SerializedCommand request) {
        requestCount.incrementAndGet();
        futureResponse = new CompletableFuture<>();
        return Mono.fromCompletionStage(futureResponse);
    }

    @Override
    public void commandResponse(SerializedCommandResponse theCommand) {
        futureResponse.complete(theCommand);
    }

    @Override
    public String getMessagingServerName() {
        return null;
    }

    public int requests() {
        return requestCount.get();
    }
}
