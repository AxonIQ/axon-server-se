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

import static io.axoniq.cli.CommandOptions.PROPERTIES;

/**
 * Handler for the register-application command.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public class RegisterApplication extends AxonIQCliCommand {

    private static final int MIN_LENGTH = 16;

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.APPLICATION,
                                                     CommandOptions.APPLICATION_ROLES,
                                                     CommandOptions.APPLICATION_DESCRIPTION,
                                                     CommandOptions.TOKEN,
                                                     PROPERTIES,
                                                     CommandOptions.SET_TOKEN);
        String url = createUrl(commandLine, "/v1/applications");

        if( commandLine.hasOption(CommandOptions.SET_TOKEN.getOpt())) {
            if( commandLine.getOptionValue(CommandOptions.SET_TOKEN.getOpt()).length() < MIN_LENGTH) {
                System.err.printf("Token must be at least %d characters", MIN_LENGTH);
                return;
            }
        }

        Application application = new Application(commandLine.getOptionValue(CommandOptions.APPLICATION.getOpt()),
                                                  commandLine.getOptionValue(CommandOptions.APPLICATION_DESCRIPTION
                                                                                     .getOpt()),
                                                  commandLine.getOptionValue(CommandOptions.SET_TOKEN.getOpt()),
                                                  commandLine
                                                          .getOptionValues(CommandOptions.APPLICATION_ROLES.getOpt()));

        if (commandLine.hasOption(PROPERTIES.getOpt())) {
            for (String metadataProperty : commandLine.getOptionValues(PROPERTIES.getOpt())) {
                String[] keyValue = metadataProperty.split("=", 2);
                if (keyValue.length != 2) {
                    throw new IllegalArgumentException("Property value must be key=value - " + keyValue);
                }
                application.getMetaData().put(keyValue[0].trim(), keyValue[1].trim());
            }
        }

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            String response = postJSON(httpclient, url, application, 200, getToken(commandLine));
            System.out.println("AccessToken is: " + response);
            System.out.println("Please note this token as this will only be provided once!");
        }
    }
}
