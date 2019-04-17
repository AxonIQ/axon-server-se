/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import io.axoniq.cli.json.ClusterNode;
import io.axoniq.cli.json.RestResponse;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.axoniq.cli.CommandOptions.*;

/**
 * @author Marc Gathier
 */
public class RegisterNode extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, INTERNALHOST, CommandOptions.INTERNALPORT, CONTEXT_TO_REGISTER_IN,
                                                     CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/cluster");
        Number port = null;
        try {
            port = (Number)commandLine.getParsedOptionValue(INTERNALPORT.getOpt());
        } catch (ParseException e) {
            throw new RuntimeException("Invalid value for option " + INTERNALPORT);
        }
        if( port == null) port = 8224;

        ClusterNode clusterNode = new ClusterNode(commandLine.getOptionValue(INTERNALHOST.getOpt().charAt(0)),
                port.intValue());

        if( commandLine.hasOption(CONTEXT_TO_REGISTER_IN.getOpt())) {
            String context = commandLine.getOptionValue(CONTEXT_TO_REGISTER_IN.getOpt());
            if(! context.isEmpty()) {
                clusterNode.setContext(context);
            }
        }


        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            postJSON(httpclient, url, clusterNode, 202, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()),
                     RestResponse.class);
        }
    }
}
