/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.command;

import io.axoniq.axonserver.commandprocesing.imp.CommandSubscriptionCache;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.serializer.Media;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by Sara Pellegrini on 19/03/2018. sara.pellegrini@gmail.com
 */
public class DefaultCommands implements Iterable<ComponentCommand> {

    private final CommandSubscriptionCache registrationCache;
    private final String component;

    public DefaultCommands(CommandSubscriptionCache registrationCache, String component) {
        this.registrationCache = registrationCache;
        this.component = component;
    }

    @Override
    public Iterator<ComponentCommand> iterator() {
        Set<io.axoniq.axonserver.commandprocessing.spi.CommandHandler> all = registrationCache.get(component);

        return all.stream().map(e -> (ComponentCommand) new MyComponentCommand(e)).iterator();
    }

    private class MyComponentCommand implements ComponentCommand {

        private final io.axoniq.axonserver.commandprocessing.spi.CommandHandler e;

        public MyComponentCommand(CommandHandler e) {
            this.e = e;
        }

        @Override
        public void printOn(Media media) {
            media.with("name", e.commandName());
        }

        @Override
        public Boolean belongsToComponent(String component) {
            return component.equals(DefaultCommands.this.component);
        }

        @Override
        public boolean belongsToContext(String context) {
            return e.context().equals(context);
        }
    }
}
