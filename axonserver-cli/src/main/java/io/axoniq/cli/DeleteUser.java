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
 * @author Marc Gathier
 */
public class DeleteUser extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.USERNAME, CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/users", CommandOptions.USERNAME);

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            delete(httpclient, url, 200, getToken(commandLine));
        }
    }
}
