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
import java.util.HashMap;
import java.util.Map;

import static io.axoniq.cli.CommandOptions.CONTEXT;
import static io.axoniq.cli.CommandOptions.PROPERTIES;

/**
 * @author Marc Gathier
 */
public class UpdateContextProperties extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0],
                                                     args,
                                                     CONTEXT,
                                                     PROPERTIES,
                                                     CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/context/" + commandLine.getOptionValue(CONTEXT.getOpt()));
        Map<String,String> properties = new HashMap<>();
        if (commandLine.hasOption(PROPERTIES.getOpt())) {
            for (String metadataProperty : commandLine.getOptionValues(PROPERTIES.getOpt())) {
                String[] keyValue = metadataProperty.split("=", 2);
                if (keyValue.length != 2) {
                    throw new IllegalArgumentException("Property value must be key=value - " + keyValue);
                }
                properties.put(keyValue[0].trim(), keyValue[1].trim());
            }
        }


        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            patchJSON(httpclient, url, properties, 200, getToken(commandLine), RestResponse.class);
        }
    }

}
