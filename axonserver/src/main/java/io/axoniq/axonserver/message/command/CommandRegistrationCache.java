/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeCommand;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents.UnsubscribeCommand;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.command.hashing.ConsistentHashRoutingSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static java.util.Collections.emptyMap;

/**
 * Registers the commands registered per client/context.
 *
 * @author Marc Gathier
 */
@Component("CommandRegistrationCache")
public class CommandRegistrationCache {

    private final Logger logger = LoggerFactory.getLogger(CommandRegistrationCache.class);
    private final ConcurrentMap<ClientStreamIdentification, CommandHandler> commandHandlersPerClientContext = new ConcurrentHashMap<>();
    private final ConcurrentMap<ClientStreamIdentification, Map<String, Integer>> registrationsPerClient = new ConcurrentHashMap<>();
    private final ConcurrentMap<CommandTypeIdentifier, RoutingSelector<String>> routingSelectors = new ConcurrentHashMap<>();

    private final Function<CommandTypeIdentifier, RoutingSelector<String>> selectorFactory;
    private final BiFunction<Map<String, MetaDataValue>, Set<ClientStreamIdentification>, Set<ClientStreamIdentification>> metaDataBasedNodeSelector;

    /**
     * Autowired constructor.
     *
     * @param metaDataBasedNodeSelector function that filters the possible clients based on meta data values in the
     *                                  request
     */
    @Autowired
    public CommandRegistrationCache(
            BiFunction<Map<String, MetaDataValue>, Set<ClientStreamIdentification>, Set<ClientStreamIdentification>> metaDataBasedNodeSelector) {
        this.selectorFactory = command -> new ConsistentHashRoutingSelector(loadFactorSolver(command));
        this.metaDataBasedNodeSelector = metaDataBasedNodeSelector;
    }

    /**
     * Default constructor. Does not filter targets based on the metadata of the command.
     */
    public CommandRegistrationCache() {
        this.selectorFactory = command -> new ConsistentHashRoutingSelector(loadFactorSolver(command));
        this.metaDataBasedNodeSelector = (metaData, targets) -> targets;
    }

    /**
     * Constructor with a custom selector factory. Does not filter targets based on the metadata of the command.
     *
     * @param selectorFactory function to find a command handling target based on the command type
     */
    public CommandRegistrationCache(Function<CommandTypeIdentifier, RoutingSelector<String>> selectorFactory) {
        this(selectorFactory, (metaData, targets) -> targets);
    }

    /**
     * Constructor specifying a specific selectorFactory and metaDataBasedNodeSelector.
     *
     * @param selectorFactory           function to find a command handling target based on the command type
     * @param metaDataBasedNodeSelector function that filters the possible clients based on meta data values in the
     *                                  request
     */
    public CommandRegistrationCache(Function<CommandTypeIdentifier, RoutingSelector<String>> selectorFactory,
                                    BiFunction<Map<String, MetaDataValue>, Set<ClientStreamIdentification>, Set<ClientStreamIdentification>> metaDataBasedNodeSelector) {
        this.selectorFactory = selectorFactory;
        this.metaDataBasedNodeSelector = metaDataBasedNodeSelector;
    }

    /**
     * Removes all registrations for a client
     *
     * @param client the clientId to remove
     */
    public void remove(ClientStreamIdentification client) {
        logger.trace("Remove {}", client);
        commandHandlersPerClientContext.remove(client);
        Map<String, Integer> remove = registrationsPerClient.remove(client);
        if (remove != null) {
            Set<String> commands = remove.keySet();
            commands.forEach(command -> routingSelector(client.getContext(), command)
                    .unregister(client.getClientStreamId()));
        }
    }

    /**
     * Removes a particular command registration for a client within a context
     *
     * @param client  the client handling the command
     * @param command the command that was registered
     */
    public void remove(ClientStreamIdentification client, String command) {
        logger.trace("Remove command {} from {}", command, client);
        Map<String, Integer> registrations = registrationsPerClient.computeIfPresent(client, (c, commandsMap) -> {
            commandsMap.remove(command);
            if (commandsMap.isEmpty()) {
                return null;
            }
            return commandsMap;
        });
        routingSelector(client.getContext(), command).unregister(client.getClientStreamId());
        if (registrations == null) {
            remove(client);
        }
    }

    /**
     * Registers a command provider. If it is an unknown handler it will be added to the consistent hash for the context
     * @param command the name of the command
     * @param commandHandler the handler of the command
     */
    public void add(String command, CommandHandler commandHandler) {
        add(command, commandHandler, 100);
    }

    private void add(String command, CommandHandler commandHandler, int loadFactor) {
        logger.trace("Add command {} to {} with load factor {}",
                     command,
                     commandHandler.clientStreamIdentification,
                     loadFactor);
        ClientStreamIdentification clientIdentification = commandHandler.getClientStreamIdentification();
        String context = clientIdentification.getContext();
        registrationsPerClient.computeIfAbsent(clientIdentification, key -> new ConcurrentHashMap<>()).put(command,
                                                                                                           loadFactor);
        commandHandlersPerClientContext.putIfAbsent(clientIdentification, commandHandler);
        routingSelector(context, command).register(clientIdentification.getClientStreamId());
    }


    /**
     * Get all registrations per connection
     * @return map of command per client connection
     */
    public Map<CommandHandler, Set<RegistrationEntry>> getAll() {
        Map<CommandHandler, Set<RegistrationEntry>> resultMap = new HashMap<>();
        commandHandlersPerClientContext.forEach(
                (contextClient, commandHandler) -> resultMap.put(commandHandler,
                                                                 registrationsPerClient.get(contextClient)
                                                                                       .entrySet()
                                                                                       .stream()
                                                                                       .map(command -> new RegistrationEntry(
                                                                                               contextClient
                                                                                                       .getContext(),
                                                                                               command.getKey(),
                                                                                               command.getValue()))
                                                                                       .collect(Collectors.toSet())));
        return resultMap;
    }

    /**
     * Gets all commands handled by a specific client
     *
     * @param clientNode the client identification
     * @return a set of commandName/context values
     */
    public Set<String> getCommandsFor(ClientStreamIdentification clientNode) {
        return registrationsPerClient.get(clientNode).keySet();
    }

    /**
     * Retrieves the client to route a specific command request to, based on its routing key. AxonServer sends requests with same routing key to the same client.
     * @param context the context in which the command is requested
     * @param request the command name
     * @param routingKey the routing key
     * @return a command handler for the command (or null of none found)
     */
    public CommandHandler getHandlerForCommand(String context, Command request, String routingKey) {
        String command = request.getName();
        Set<ClientStreamIdentification> candidates = getCandidates(context, request);
        if (candidates.isEmpty()) {
            return null;
        }
        if (candidates.size() == 1) {
            return commandHandlersPerClientContext.get(candidates.iterator().next());
        }
        Set<String> candidateNames = candidates.stream().map(ClientStreamIdentification::getClientStreamId).collect(
                Collectors
                        .toSet());
        RoutingSelector<String> routingSelector = routingSelector(context, command);
        return routingSelector
                .selectHandler(routingKey, candidateNames)
                .map(client -> commandHandlersPerClientContext.get(new ClientStreamIdentification(context, client)))
                .orElse(null);
    }

    private Set<ClientStreamIdentification> getCandidates(String context, Command command) {

        Set<ClientStreamIdentification> candidates = registrationsPerClient.entrySet()
                                                                           .stream()
                                                                           .filter(entry -> context.equals(entry.getKey().getContext()))
                                                                           .filter(entry -> entry
                                                                                   .getValue()
                                                                                   .containsKey(
                                                                                           command.getName()))
                                                                           .map(Map.Entry::getKey)
                                                                           .collect(
                                                                                   Collectors
                                                                                           .toSet());
        return metaDataBasedNodeSelector.apply(command.getMetaDataMap(), candidates);
    }


    @Nonnull
    private RoutingSelector<String> routingSelector(String context, String command) {
        CommandTypeIdentifier commandIdentification = new CommandTypeIdentifier(context, command);
        return routingSelectors.computeIfAbsent(commandIdentification, selectorFactory::apply);
    }

    private Function<String, Integer> loadFactorSolver(CommandTypeIdentifier command) {
        return client -> registrationsPerClient.getOrDefault(new ClientStreamIdentification(command.context(),
                                                                                            client),
                                                             emptyMap())
                                               .getOrDefault(command.name(), 0);
    }

    /**
     * Find the command handler for a command based on the specified client/context
     *
     * @param clientIdentification the client identification
     * @param request              the command name
     * @return the command handler for the request, or null when not found
     */
    public CommandHandler findByClientAndCommand(ClientStreamIdentification clientIdentification, String request) {
        boolean found = registrationsPerClient.getOrDefault(clientIdentification, emptyMap()).containsKey(request);
        if (!found) {
            return null;
        }
        return commandHandlersPerClientContext.get(clientIdentification);
    }

    @EventListener
    public void on(SubscribeCommand event) {
        CommandSubscription request = event.getRequest();
        int loadFactor = request.getLoadFactor() == 0 ? 100 : request.getLoadFactor();
        add(request.getCommand(), event.getHandler(), loadFactor);
    }

    @EventListener
    public void on(UnsubscribeCommand event) {
        CommandSubscription request = event.getRequest();
        remove(event.clientIdentification(), request.getCommand());
    }


    public static class RegistrationEntry {
        private final String command;
        private final String context;
        private final int loadFactor;

        public RegistrationEntry(String context, String command) {
            this(context, command, 100);
        }

        public RegistrationEntry(String context, String command, int loadFactor) {
            this.command = command;
            this.context = context;
            this.loadFactor = loadFactor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RegistrationEntry that = (RegistrationEntry) o;
            return Objects.equals(command, that.command) &&
                    Objects.equals(context, that.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(command, context);
        }

        public String getCommand() {
            return command;
        }

        public String getContext() {
            return context;
        }

        public int getLoadFactor() {
            return loadFactor;
        }
    }
}
