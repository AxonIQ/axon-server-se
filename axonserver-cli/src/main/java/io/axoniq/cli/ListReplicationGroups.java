/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import io.axoniq.cli.json.ReplicationGroupJSON;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * @author Marc Gathier
 */
public class ListReplicationGroups extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/public/replicationgroups");

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            if (jsonOutput(commandLine)) {
                System.out.println(getJSON(httpclient,
                                           url,
                                           String.class,
                                           200,
                                           getToken(commandLine)));
            } else {
                ReplicationGroupJSON[] replicationGroups = getJSON(httpclient,
                                                                   url,
                                                                   ReplicationGroupJSON[].class,
                                                                   200,
                                                                   getToken(commandLine));
                System.out.printf("%-20s %-20s %-60s%n", "Name", "Leader", "Members");

                for (ReplicationGroupJSON replicationGroup : replicationGroups) {
                    System.out.printf("%-20s %-20s %-60s%n", replicationGroup.getName(),
                                      replicationGroup.getLeader(),
                                      replicationGroup.concatRoles()
                    );
                }
            }
        }
    }
}
