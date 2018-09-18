package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.DispatchEvents;
import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.SubscriptionEvents;
import io.axoniq.axonserver.TopologyEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ErrorMessageFactory;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Responsible for managing command subscriptions and processing commands.
 * Subscriptions are stored in the {@link CommandRegistrationCache}.
 * Running commands are stored in the {@link CommandCache}.
 *
 * Author: marc
 */
@Component("CommandDispatcher")
public class CommandDispatcher {

    private static final String COMMAND_COUNTER_NAME = "axon.commands.count";
    private static final String ACTIVE_COMMANDS_GAUGE = "axon.commands.active";
    private final CommandRegistrationCache registrations;
    private final CommandCache commandCache;
    private final CommandMetricsRegistry metricRegistry;
    private final Logger logger = LoggerFactory.getLogger(CommandDispatcher.class);
    private final FlowControlQueues<WrappedCommand> commandQueues = new FlowControlQueues<>(Comparator.comparing(WrappedCommand::priority).reversed());
    private final Counter commandCounter;

    public CommandDispatcher(CommandRegistrationCache registrations, CommandCache commandCache, CommandMetricsRegistry metricRegistry) {
        this.registrations = registrations;
        this.commandCache = commandCache;
        this.metricRegistry = metricRegistry;
        this.commandCounter = metricRegistry.counter(COMMAND_COUNTER_NAME);
        metricRegistry.gauge(ACTIVE_COMMANDS_GAUGE, commandCache, ConcurrentHashMap::size);
    }

    @EventListener
    public void on(SubscriptionEvents.SubscribeCommand event) {
        CommandSubscription request = event.getRequest();
        registrations.add(event.getContext(), request.getCommand(), event.getHandler());
        event.getHandler().confirm(request.getMessageId());
    }

    @EventListener
    public void on(SubscriptionEvents.UnsubscribeCommand event) {
        CommandSubscription request = event.getRequest();
        CommandHandler commandHandler = registrations.remove(event.getContext(),
                                                             request.getCommand(),
                                                             request.getClientName());
        if (commandHandler != null) {
            commandHandler.confirm(request.getMessageId());
        }
    }

    @EventListener
    public void on(DispatchEvents.DispatchCommand dispatchCommand) {
        dispatch(dispatchCommand.getContext(), dispatchCommand.getRequest(), dispatchCommand.getResponseObserver(), dispatchCommand.isProxied());
    }

    public void dispatch(String context, Command request, Consumer<CommandResponse> responseObserver, boolean proxied) {
        if( proxied) {
            String client = ProcessingInstructionHelper.targetClient(request.getProcessingInstructionsList());
            context = ProcessingInstructionHelper.context(request.getProcessingInstructionsList());
            CommandHandler handler = registrations.findByClientAndCommand(client, request);
            dispatchToCommandHandler(context, request, handler, responseObserver);
        } else {
            commandCounter.increment();
            CommandHandler commandHandler = registrations.getNode(context, request,
                                                                  ProcessingInstructionHelper.routingKey(request.getProcessingInstructionsList()));
            Command command = Command.newBuilder(request).addProcessingInstructions(ProcessingInstructionHelper.targetClient(commandHandler == null? "" : commandHandler.getClient())).build();
            dispatchToCommandHandler(context, command, commandHandler, responseObserver);
        }
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        cleanupRegistrations(event.getClient());
        if( ! event.isProxied()) {
            getCommandQueues().move(event.getClient(), this::redispatch);
        }
        handlePendingCommands(event.getClient());
    }

    //TODO: move code
//    @EventListener
//    public void on(ClusterEvents.AxonHubInstanceDisconnected event) {
//        getCommandQueues().move(event.getNodeName(), this::redispatch);
//    }

    private void dispatchToCommandHandler(String context, Command command, CommandHandler commandHandler,
                                          Consumer<CommandResponse> responseObserver) {
        if (commandHandler == null) {
            responseObserver.accept(CommandResponse.newBuilder()
                                                   .setMessageIdentifier(command.getMessageIdentifier())
                                                   .setRequestIdentifier(command.getMessageIdentifier())
                                                   .setErrorCode(ErrorCode.NO_HANDLER_FOR_COMMAND.getCode())
                                                   .setMessage(ErrorMessageFactory.build("No Handler for command: " + command.getName()))
                                                   .build());
            return;
        }

        logger.debug("Dispatch {} to: {}", command.getName(), commandHandler.getClient());
        commandCache.put(command.getMessageIdentifier(), new CommandInformation(command.getName(), responseObserver, commandHandler.getClient(), commandHandler.getComponentName()));
        commandQueues.put(commandHandler.queueName(), new WrappedCommand(context, command));
    }


    public void handleResponse(CommandResponse commandResponse, boolean proxied) {
        CommandInformation toPublisher = commandCache.remove(commandResponse.getRequestIdentifier());
        if (toPublisher != null) {
            logger.debug("Sending response to: {}", toPublisher);
            if (!proxied) {
                metricRegistry.add(toPublisher.getCommand(), toPublisher.getClientId(), System.currentTimeMillis() - toPublisher.getTimestamp());
            }
            toPublisher.getResponseConsumer().accept(commandResponse);
        } else {
            logger.info("Could not find command request: {}", commandResponse.getRequestIdentifier());
        }

    }

    private void cleanupRegistrations(String client) {
        registrations.remove(client);
    }

    public FlowControlQueues<WrappedCommand> getCommandQueues() {
        return commandQueues;
    }

    private String redispatch(WrappedCommand command) {
        Command request = command.command();
        CommandInformation commandInformation = commandCache.remove(request.getMessageIdentifier());
        if( commandInformation == null) return null;

        CommandHandler client = registrations.getNode(command.context(), request, ProcessingInstructionHelper.routingKey(request.getProcessingInstructionsList()));
        if (client == null) {
            commandInformation.getResponseConsumer().accept(CommandResponse.newBuilder()
                    .setMessageIdentifier(request.getMessageIdentifier()).setRequestIdentifier(request.getMessageIdentifier())
                    .setErrorCode(ErrorCode.NO_HANDLER_FOR_COMMAND.getCode())
                    .setMessage(ErrorMessageFactory.build("No Handler for command: " + request.getName()))
                    .build());
            return null;
        }

        logger.debug("Dispatch {} to: {}", request.getName(), client.getClient());

        commandCache.put(request.getMessageIdentifier(), new CommandInformation(request.getName(), commandInformation.getResponseConsumer(),
                client.getClient(), client.getComponentName()));
        return client.queueName();
    }

    private void handlePendingCommands(String client) {
        List<String> messageIds = commandCache.entrySet().stream().filter(e -> e.getValue().getClientId().equals(client)).map(Map.Entry::getKey).collect(Collectors.toList());

        messageIds.forEach(m -> {
            CommandInformation ci = commandCache.remove(m);
            if( ci != null) {
                ci.getResponseConsumer().accept(CommandResponse.newBuilder()
                        .setMessageIdentifier(m)
                                                               .setRequestIdentifier(m)
                        .setMessage(ErrorMessageFactory.build("Connection lost while executing command on: " + ci.getClientId()))
                        .setErrorCode(ErrorCode.CONNECTION_TO_HANDLER_LOST.getCode())
                        .build());
            }
        });
    }

    public long getNrOfCommands() {
        return (long)commandCounter.count();
    }

    public int commandCount() {
        return commandCache.size();
    }


}
