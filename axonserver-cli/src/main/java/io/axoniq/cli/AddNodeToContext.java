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

import static io.axoniq.cli.CommandOptions.CONTEXT;
import static io.axoniq.cli.CommandOptions.NODENAME;

/**
 * @author Marc Gathier
 */
public class AddNodeToContext extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CONTEXT, NODENAME, CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/context", CONTEXT, NODENAME);

        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            RestResponse response = postJSON(httpclient, url, null, 202, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()),
                     RestResponse.class);
            System.out.println(response.getMessage());
        }
    }
}
