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
 * CLI handler for the unregister-plugin command. Unregisters an plugin for a specific context.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class UnregisterPlugin extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.PLUGIN_NAME,
                                                     CommandOptions.PLUGIN_VERSION,
                                                     CommandOptions.PLUGIN_CONTEXT,
                                                     CommandOptions.TOKEN);

        StringBuilder queryString = new StringBuilder("?name=").append(option(commandLine,
                                                                              CommandOptions.PLUGIN_NAME))
                                                               .append("&version=").append(option(commandLine,
                                                                                                  CommandOptions.PLUGIN_VERSION));

        if (commandLine.hasOption(CommandOptions.PLUGIN_CONTEXT.getOpt())) {
            queryString.append("&targetContext=").append(option(commandLine, CommandOptions.PLUGIN_CONTEXT));
        }
        String url = createUrl(commandLine, "/v1/plugins/context" + queryString.toString());

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            delete(httpclient, url, 200, getToken(commandLine));
        }
    }
}
