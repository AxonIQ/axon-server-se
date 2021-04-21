/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.refactoring.messaging.api.Client;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandDefinition;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandHandler;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;
import reactor.core.publisher.Mono;

/**
 * @author Marc Gathier
 */
class DummyCommandHandler implements CommandHandler {

    private final String command;
    private final String client1;
    private final String applicationName;
    private final String context;

    public DummyCommandHandler(String command, String client1, String applicationName, String context) {
        this.command = command;
        this.client1 = client1;
        this.applicationName = applicationName;
        this.context = context;
    }

    @Override
    public CommandDefinition definition() {
        return new CommandDefinition() {
            @Override
            public String name() {
                return command;
            }

            @Override
            public String context() {
                return context;
            }
        };
    }

    @Override
    public Client client() {
        return new Client() {

            @Override
            public String id() {
                return client1;
            }

            @Override
            public String applicationName() {
                return applicationName;
            }
        };
    }

    @Override
    public Mono<CommandResponse> handle(Command command) {
        return null;
    }
}
