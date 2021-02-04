/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import io.axoniq.cli.json.ExtensionInfo;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * CLI handler for the extensions command. Returns a list of all installed extensions and the contexts they are
 * registered in.
 * For each context where an extension is registered it shows the status (Active/Registered).
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ListExtensions extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/extensions");

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            if (jsonOutput(commandLine)) {
                System.out.println(getJSON(httpclient, url, String.class, 200, getToken(commandLine)));
            } else {
                ExtensionInfo[] extensions = getJSON(httpclient,
                                                     url,
                                                     ExtensionInfo[].class,
                                                     200,
                                                     getToken(commandLine));
                showExtensionsAsTable(extensions);
            }
        }
    }

    private static void showExtensionsAsTable(ExtensionInfo[] extensions) {
        System.out.printf("%-30s %-15s %-20s %-10s%n", "Name", "Version", "Context", "Status");

        for (ExtensionInfo extensionInfo : extensions) {
            if (extensionInfo.getContextInfoList().length == 0) {
                System.out.printf("%-30s %-15s%n",
                                  extensionInfo.getName(),
                                  extensionInfo.getVersion());
            } else {
                for (int i = 0; i < extensionInfo.getContextInfoList().length; i++) {
                    ExtensionInfo.ExtensionContextInfo contextInfo = extensionInfo.getContextInfoList()[i];
                    System.out.printf("%-30s %-15s %-20s %-10s%n",
                                      i == 0 ? extensionInfo.getName() : "",
                                      i == 0 ? extensionInfo.getVersion() : "",
                                      contextInfo.getContext(),
                                      contextInfo.isActive() ? "Active" : "Registered");
                }
            }
        }
    }
}
