/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.hashing.ConsistentHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

/**
 * Registers the commands registered per client/context.
 * @author Marc Gathier
 */
@Component("CommandRegistrationCache")
public class CommandRegistrationCache {
    private final Logger logger = LoggerFactory.getLogger(CommandRegistrationCache.class);
    private final ConcurrentMap<ClientIdentification, CommandHandler> commandHandlersPerClientContext = new ConcurrentHashMap<>();
    private final ConcurrentMap<ClientIdentification, Set<String>> registrationsPerClient = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConsistentHash> consistentHashPerContext= new ConcurrentHashMap<>();

    /**
     * Removes all registrations for a client
     * @param client the clientId to remove
     */
    public void remove(ClientIdentification client) {
        logger.trace("Remove {}", client);
        commandHandlersPerClientContext.remove(client);
        consistentHashPerContext.computeIfPresent(client.getContext(),  (context,current) -> current.without(client.getClient()));
        registrationsPerClient.remove(client);
        logger.trace("Consistent hash = {}", consistentHashPerContext.get(client.getContext()));
    }

    /**
     * Removes a particular command registration for a client within a context
     * @param client the client handling the command
     * @param command the command that was registered
     */
    public void remove(ClientIdentification client, String command) {
        logger.trace("Remove command {} from {}", command, client);
        Set<String> registrations = registrationsPerClient.computeIfPresent(client, (c, set) -> {
            set.remove(command);
            if (set.isEmpty()) {
                return null;
            }
            return set;
        });
        if( registrations == null) {
            remove(client);
        }
    }

    /**
     * Registers a command provider. If it is an unknown handler it will be added to the consistent hash for the context
     * @param command the name of the command
     * @param commandHandler the handler of the command
     */
    public void add(String command, CommandHandler commandHandler) {
        logger.trace("Add command {} to {}", command, commandHandler.client);
        add(command, commandHandler, 100);
    }

    private void add(String command, CommandHandler commandHandler, int loadFactor) {
        logger.trace("Add command {} to {}", command, commandHandler.client);
        ClientIdentification clientIdentification = commandHandler.getClient();
        ConsistentHash consistentHash = consistentHashPerContext.computeIfAbsent(clientIdentification.getContext(),
                                                                                 c -> new ConsistentHash());
        if (!consistentHash.contains(clientIdentification.getClient())) {
            consistentHashPerContext.put(clientIdentification.getContext(),
                                         consistentHash.with(clientIdentification.getClient(),
                                                             loadFactor,
                                                             cmd -> provides(clientIdentification, cmd)));
        }
        logger.trace("Consistent hash = {}", consistentHashPerContext.get(clientIdentification.getContext()));
        registrationsPerClient.computeIfAbsent(clientIdentification, key -> new CopyOnWriteArraySet<>()).add(command);
        commandHandlersPerClientContext.putIfAbsent(clientIdentification, commandHandler);
    }

    private boolean provides(ClientIdentification client, String cmd) {
        return registrationsPerClient.containsKey(client) && registrationsPerClient.get(client).contains(cmd);
    }

    /**
     * Get all registrations per connection
     * @return map of command per client connection
     */
    public Map<CommandHandler, Set<RegistrationEntry>> getAll() {
        Map<CommandHandler, Set<RegistrationEntry>> resultMap = new HashMap<>();
        commandHandlersPerClientContext.forEach((contextClient, commandHandler)
                                                        -> resultMap.put(commandHandler,
                                                                         registrationsPerClient.get(contextClient)
                                                                                               .stream()
                                                                                               .map(command -> new RegistrationEntry(contextClient.getContext(), command))
                                                                                               .collect(Collectors.toSet())));
        return resultMap;
    }

    /**
     * Gets all commands handled by a specific client
     * @param clientNode the client identification
     * @return a set of commandName/context values
     */
    public Set<String> getCommandsFor(ClientIdentification clientNode) {
        return registrationsPerClient.get(clientNode);
    }

    /**
     * Retrieves the client to route a specific command request to, based on its routing key. AxonServer sends requests with same routing key to the same client.
     * @param context the context in which the command is requested
     * @param request the command name
     * @param routingKey the routing key
     * @return a command handler for the command (or null of none found)
     */
    public CommandHandler getHandlerForCommand(String context, Command request, String routingKey) {
        ConsistentHash hash = consistentHashPerContext.get(context);
        if( hash == null) return null;
        String client = hash
                                                .getMember(routingKey, request.getName())
                                                .map(ConsistentHash.ConsistentHashMember::getClient)
                                                .orElse(null);
        if( client == null ) return null;
        return commandHandlersPerClientContext.get(new ClientIdentification(context, client));
    }

    /**
     * Find the command handler for a command based on the specified client/context
     * @param clientIdentification the client identification
     * @param request the command name
     * @return the command handler for the request, or null when not found
     */
    public CommandHandler findByClientAndCommand(ClientIdentification clientIdentification, String request) {
        boolean found = registrationsPerClient.getOrDefault(clientIdentification, Collections.emptySet()).contains(request);
        if( !found) return null;
        return commandHandlersPerClientContext.get(clientIdentification);
    }

    @EventListener
    public void on(SubscriptionEvents.SubscribeCommand event) {
        CommandSubscription request = event.getRequest();
        int loadFactor = request.getLoadFactor() == 0 ? 100 : request.getLoadFactor();
        add(request.getCommand(), event.getHandler(), loadFactor);
    }

    @EventListener
    public void on(SubscriptionEvents.UnsubscribeCommand event) {
        CommandSubscription request = event.getRequest();
        remove(event.clientIdentification(),request.getCommand());
    }


    public static class RegistrationEntry {
        private final String command;
        private final String context;


        public RegistrationEntry(String context, String command) {
            this.command = command;
            this.context = context;
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
    }
}
