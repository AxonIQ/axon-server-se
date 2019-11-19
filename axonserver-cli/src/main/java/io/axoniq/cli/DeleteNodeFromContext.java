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

import static io.axoniq.cli.CommandOptions.*;

/**
 * Handler for the delete-node-from-context command.
 * @author Marc Gathier
 * @since 4.0
 */
public class DeleteNodeFromContext extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0],
                                                     args,
                                                     CONTEXT,
                                                     NODE_NAME,
                                                     PRESERVE_EVENT_STORE,
                                                     CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/context", CONTEXT, NODE_NAME);

        if (commandLine.hasOption(PRESERVE_EVENT_STORE.getLongOpt())) {
            url += "?preserveEventStore=true";
        }
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            delete(httpclient, url, 202, getToken(commandLine));
        }
    }
}
