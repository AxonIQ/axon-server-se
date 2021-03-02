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
 * CLI handler for the delete-plugin command. Deletes a plugin from Axon Server, if the plugin is still
 * active for some contexts it will be deactivated.
 *
 * @author Marc Gathier
 * @version 4.5
 */
public class DeletePlugin extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.PLUGIN_NAME,
                                                     CommandOptions.PLUGIN_VERSION,
                                                     CommandOptions.TOKEN);

        StringBuilder queryString = new StringBuilder("?name=").append(option(commandLine,
                                                                              CommandOptions.PLUGIN_NAME))
                                                               .append("&version=").append(option(commandLine,
                                                                                                  CommandOptions.PLUGIN_VERSION));

        String url = createUrl(commandLine, "/v1/plugins" + queryString.toString());

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            delete(httpclient, url, 200, getToken(commandLine));
        }
    }
}
