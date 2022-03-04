/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.List;

import static io.axoniq.cli.CommandOptions.*;
import static java.util.Arrays.asList;

/**
 * @author Marc Gathier
 */
public class DeleteContext extends AxonIQCliCommand {

    private static final List<Integer> VALID_STATUS_CODES = asList(200, 202);

    public static void run(String[] args) throws IOException {
        CommandLine commandLine = processCommandLine(args[0], args, CONTEXT, TOKEN, PRESERVE_EVENT_STORE);
        String url = createUrl(commandLine, "/v1/context", CONTEXT);
        if (commandLine.hasOption(PRESERVE_EVENT_STORE.getLongOpt())) {
            url += "?preserveEventStore=true";
        }

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            delete(httpclient, url, VALID_STATUS_CODES::contains, getToken(commandLine));
        }
    }
}
