/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import io.axoniq.cli.json.RestResponse;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

import static io.axoniq.cli.CommandOptions.*;

/**
 * Handler for CLI command add-node-to-context. Adds a node with an optional role to a context.
 * @author Marc Gathier
 * @since 4.0
 */
public class AddNodeToContext extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0],
                                                     args,
                                                     CONTEXT,
                                                     NODE_NAME,
                                                     NODE_ROLE,
                                                     CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/context", CONTEXT, NODE_NAME);
        if (commandLine.hasOption(NODE_ROLE.getOpt())) {
            url += "?role=" + commandLine.getOptionValue(NODE_ROLE.getOpt());
        }

        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            RestResponse response = postJSON(httpclient, url, null, 202, getToken(commandLine),
                                             RestResponse.class);
            System.out.println(response.getMessage());
        }
    }
}
