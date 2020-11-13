/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import io.axoniq.cli.json.User;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * @author Marc Gathier
 */
public class RegisterUser extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.USERNAME,
                CommandOptions.PASSWORD,
                CommandOptions.NO_PASSWORD,
                CommandOptions.USER_ROLES,
                CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/users");

        String password = commandLine.getOptionValue(CommandOptions.PASSWORD.getOpt());
        if((password == null) && ! commandLine.hasOption(CommandOptions.NO_PASSWORD.getLongOpt())) {
            password = PasswordReader.INSTANCE.readPassword();
        }

        User user= new User(commandLine.getOptionValue(CommandOptions.USERNAME.getOpt()),
                password, commandLine.getOptionValues(CommandOptions.USER_ROLES.getOpt()));

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            postJSON(httpclient, url, user, 200, getToken(commandLine));
        }
    }
}
