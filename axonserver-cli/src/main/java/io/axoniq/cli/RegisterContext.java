/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import io.axoniq.cli.json.ContextNode;
import io.axoniq.cli.json.RestResponse;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.axoniq.cli.CommandOptions.CONTEXT;
import static io.axoniq.cli.CommandOptions.NODES;

/**
 * @author Marc Gathier
 */
public class RegisterContext extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CONTEXT, NODES, CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/context");

        List<String> nodeRolesMap = new ArrayList<>();
        if( commandLine.hasOption(CommandOptions.NODES.getOpt()) ) {
            nodeRolesMap.addAll(Arrays.asList(commandLine.getOptionValues(CommandOptions.NODES.getOpt())));

        }

        ContextNode clusterNode = new ContextNode(commandLine.getOptionValue(CONTEXT.getOpt()), nodeRolesMap);

        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            postJSON(httpclient, url, clusterNode, 200, getToken(commandLine),
                     RestResponse.class);
        }
    }
}
