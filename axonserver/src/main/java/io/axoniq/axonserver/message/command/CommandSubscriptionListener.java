/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

/**
 * @author Marc Gathier
 */
public interface CommandSubscriptionListener {
    void unsubscribeCommand(String context, String command, String client, String componentName);
    void subscribeCommand(String context, String command, String client, String componentName);

}
