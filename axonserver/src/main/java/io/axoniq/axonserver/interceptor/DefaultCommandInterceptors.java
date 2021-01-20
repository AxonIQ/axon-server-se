/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionServiceProvider;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.Ordered;
import io.axoniq.axonserver.extensions.ServiceWithInfo;
import io.axoniq.axonserver.extensions.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.CommandResponseInterceptor;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Bundles the interceptors for commands in a single component.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class DefaultCommandInterceptors implements CommandInterceptors {

    private final Logger logger = LoggerFactory.getLogger(DefaultCommandInterceptors.class);

    private final List<ServiceWithInfo<CommandRequestInterceptor>> commandRequestInterceptors = new CopyOnWriteArrayList<>();
    private final List<ServiceWithInfo<CommandResponseInterceptor>> commandResponseInterceptors = new CopyOnWriteArrayList<>();
    private final ExtensionServiceProvider osgiController;
    private final ExtensionContextFilter extensionContextFilter;
    private volatile boolean initialized;


    public DefaultCommandInterceptors(ExtensionServiceProvider osgiController,
                                      ExtensionContextFilter extensionContextFilter) {
        this.osgiController = osgiController;
        this.extensionContextFilter = extensionContextFilter;
        osgiController.registerExtensionListener(serviceEvent -> {
            logger.debug("service event {}", serviceEvent);
            initialized = false;
        });
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (osgiController) {
                if (initialized) {
                    return;
                }
                initHooks(CommandRequestInterceptor.class, commandRequestInterceptors);
                initHooks(CommandResponseInterceptor.class, commandResponseInterceptors);

                initialized = true;
            }
        }
    }

    private <T extends Ordered> void initHooks(Class<T> clazz, List<ServiceWithInfo<T>> hooks) {
        hooks.clear();
        hooks.addAll(osgiController.getServicesWithInfo(clazz));
        hooks.sort(Comparator.comparingInt(ServiceWithInfo::order));
        logger.debug("{} {}}", hooks.size(), clazz.getSimpleName());
    }

    @Override
    public SerializedCommand commandRequest(SerializedCommand serializedCommand,
                                            ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        if (commandRequestInterceptors.isEmpty()) {
            return serializedCommand;
        }
        Command command = serializedCommand.wrapped();
        for (ServiceWithInfo<CommandRequestInterceptor> commandRequestInterceptor : commandRequestInterceptors) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), commandRequestInterceptor.extensionKey())) {
                command = commandRequestInterceptor.service().commandRequest(command, extensionUnitOfWork);
            }
        }
        return new SerializedCommand(command);
    }

    @Override
    public SerializedCommandResponse commandResponse(SerializedCommandResponse serializedResponse,
                                                     ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        if (commandResponseInterceptors.isEmpty()) {
            return serializedResponse;
        }
        CommandResponse response = serializedResponse.wrapped();
        try {
            for (ServiceWithInfo<CommandResponseInterceptor> commandResponseInterceptor : commandResponseInterceptors) {
                if (extensionContextFilter.test(extensionUnitOfWork.context(),
                                                commandResponseInterceptor.extensionKey())) {
                    response = commandResponseInterceptor.service().commandResponse(response, extensionUnitOfWork);
                }
            }
        } catch (Exception ex) {
            logger.warn("{}@{} an exception occurred in a CommandResponseInterceptor", extensionUnitOfWork.principal(),
                        extensionUnitOfWork.context(), ex);
        }

        return new SerializedCommandResponse(response);
    }
}
