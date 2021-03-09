/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import io.axoniq.cli.json.PluginInfo;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * CLI handler for the plugins command. Returns a list of all installed plugins and the contexts they are
 * registered in.
 * For each context where an plugin is registered it shows the status (Active/Registered).
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ListPlugins extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/plugins");

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            if (jsonOutput(commandLine)) {
                System.out.println(getJSON(httpclient, url, String.class, 200, getToken(commandLine)));
            } else {
                PluginInfo[] extensions = getJSON(httpclient,
                                                  url,
                                                  PluginInfo[].class,
                                                  200,
                                                  getToken(commandLine));
                showExtensionsAsTable(extensions);
            }
        }
    }

    private static void showExtensionsAsTable(PluginInfo[] plugins) {
        System.out.printf("%-30s %-15s %-20s %-10s%n", "Name", "Version", "Context", "Status");

        for (PluginInfo pluginInfo : plugins) {
            if (pluginInfo.getContextInfoList().length == 0) {
                System.out.printf("%-30s %-15s%n",
                                  pluginInfo.getName(),
                                  pluginInfo.getVersion());
            } else {
                for (int i = 0; i < pluginInfo.getContextInfoList().length; i++) {
                    PluginInfo.PluginContextInfo contextInfo = pluginInfo.getContextInfoList()[i];
                    System.out.printf("%-30s %-15s %-20s %-10s%n",
                                      i == 0 ? pluginInfo.getName() : "",
                                      i == 0 ? pluginInfo.getVersion() : "",
                                      contextInfo.getContext(),
                                      contextInfo.isActive() ? "Active" : "Registered");
                }
            }
        }
    }
}
