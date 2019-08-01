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
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.util.Arrays;
import java.util.Comparator;

/**
 * @author Marc Gathier
 */
public class Cluster extends AxonIQCliCommand {
    public static void run(String[] args)  {
        // check args

        CommandLine commandLine = processCommandLine( args[0], args, CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/public");

        try (CloseableHttpClient httpclient  = createClient(commandLine)) {
            if( jsonOutput(commandLine)) {
                System.out.println(getJSON(httpclient,
                                           url,
                                           String.class,
                                           200,
                                           getToken(commandLine)));
            } else {
                ClusterNode[] nodes = getJSON(httpclient,
                                              url,
                                              ClusterNode[].class,
                                              200,
                                              getToken(commandLine));
                System.out.printf("%-40s %-40s %10s %10s %s\n",
                                  "Name",
                                  "Hostname",
                                  "gRPC Port",
                                  "HTTP Port",
                                  "Connected");
                Arrays.stream(nodes).sorted(Comparator.comparing(ClusterNode::getName)).forEach(node -> {
                    System.out.printf("%-40s %-40s %10d %10d %s\n",
                                      node.getName(),
                                      node.getHostName(),
                                      node.getGrpcPort(),
                                      node.getHttpPort(),
                                      node.isConnected() ? "*" : "");
                });
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(url, e);
        }
    }


}
