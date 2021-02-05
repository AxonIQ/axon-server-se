/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * CLI handler for the unregister-extension command. Unregisters an extension for a specific context.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class UnregisterExtension extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.EXTENSION_NAME,
                                                     CommandOptions.EXTENSION_VERSION,
                                                     CommandOptions.EXTENSION_CONTEXT,
                                                     CommandOptions.TOKEN);

        StringBuilder queryString = new StringBuilder("?extension=").append(option(commandLine,
                                                                                   CommandOptions.EXTENSION_NAME))
                                                                    .append("&version=").append(option(commandLine,
                                                                                                       CommandOptions.EXTENSION_VERSION));

        if (commandLine.hasOption(CommandOptions.EXTENSION_CONTEXT.getOpt())) {
            queryString.append("&targetContext=").append(option(commandLine, CommandOptions.EXTENSION_CONTEXT));
        }
        String url = createUrl(commandLine, "/v1/extensions/context" + queryString.toString());

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            delete(httpclient, url, 200, getToken(commandLine));
        }
    }
}
