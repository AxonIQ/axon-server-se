/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import io.axoniq.cli.json.Application;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class ListApplications extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/public/applications");

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            if( jsonOutput(commandLine) ) {
                System.out.println(getJSON(httpclient, url, String.class, 200, getToken(commandLine)));
            } else {
                Application[] applications = getJSON(httpclient,
                                                     url,
                                                     Application[].class,
                                                     200,
                                                     getToken(commandLine));
                System.out.printf("%-20s %-60s %-20s%n", "Name", "Description", "Roles");

                for (Application app : applications) {
                    System.out.printf("%-20s %-60s %-20s%n",
                                      app.getName(),
                                      app.getDescription() != null ? app.getDescription() : "",
                                      app.getRoles().stream().map(Object::toString).collect(Collectors.joining(",")));
                }
            }
        }
    }

}
