package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.transport.grpc.command.GrpcCommandResult;
import io.axoniq.axonserver.transport.grpc.command.GrpcMapper;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Stefan Dragisic
 */
class GrpcCommandHandlerSubscription implements CommandHandlerSubscription {

    private final CommandHandler commandHandler;
    private final Function<Command, Mono<CommandResponse>> dispatchOperation;
    private final String clientId;

    public GrpcCommandHandlerSubscription(CommandSubscription subscription,
                                          String clientId,
                                          String context,
                                          Supplier<Map<String, String>> clientTagsProvider,
                                          Function<Command, Mono<CommandResponse>> dispatchOperation) {
        this.clientId = clientId;
        this.commandHandler = new GrpcCommandHandler(subscription,context,clientTagsProvider);
        this.dispatchOperation = dispatchOperation;
    }

    @Override
    public CommandHandler commandHandler() {
        return commandHandler;
    }

    //dispatches command from axon server to command handler (via command handler stream)
    @Override
    public Mono<CommandResult> dispatch(io.axoniq.axonserver.commandprocessing.spi.Command command) {
        return dispatchOperation.apply(GrpcMapper.map(command)).map(result -> map(result, clientId));
    }

    private CommandResult map(CommandResponse commandResponse, String clientId) {
        CommandResponse response = CommandResponse.newBuilder(commandResponse)
                .putMetaData(CommandResult.CLIENT_ID,
                        MetaDataValue.newBuilder().setTextValue(clientId)
                                .build())
                .build();
        return new GrpcCommandResult(response);
    }

    private static class GrpcCommandHandler implements CommandHandler {

        private final CommandSubscription subscription;
        private final String context;
        private final Supplier<Map<String, String>> clientTagsProvider;
        private final String id = UUID.randomUUID().toString();

        private GrpcCommandHandler(CommandSubscription subscription,
                                   String context, Supplier<Map<String, String>> clientTagsProvider) {
            this.subscription = subscription;
            this.context = context;
            this.clientTagsProvider = clientTagsProvider;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public String description() {
            return subscription.getCommand() + " at " + subscription.getClientId();
        }

        @Override
        public String commandName() {
            return subscription.getCommand();
        }

        @Override
        public String context() {
            return context;
        }

        @Override
        public Metadata metadata() {
            Map<String, Serializable> clientMetadata = new HashMap<>(clientTagsProvider.get());
            clientMetadata.put(LOAD_FACTOR, subscription.getLoadFactor());
            clientMetadata.put(CLIENT_ID, subscription.getClientId());
            clientMetadata.put(COMPONENT_NAME, subscription.getComponentName());
            return new Metadata() {
                @Override
                public Iterable<String> metadataKeys() {
                    return clientMetadata.keySet();
                }

                @Override
                public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                    return Optional.ofNullable((R) clientMetadata.get(metadataKey));
                }
            };
        }
    }
}

