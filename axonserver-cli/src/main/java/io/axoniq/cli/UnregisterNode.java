/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
 * Handler for the unregister-node command.
 * @author Marc Gathier
 * @since 4.0
 */
public class UnregisterNode extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args,
                                                     CommandOptions.NODE_NAME, CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/cluster", CommandOptions.NODE_NAME);
        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            delete(httpclient, url, 200, getToken(commandLine));
        }
    }
}
