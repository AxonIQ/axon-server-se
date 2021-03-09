/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import io.axoniq.cli.json.PluginConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static io.axoniq.cli.CommandOptions.PROPERTIES;

/**
 * CLI handler for the configure-plugin command. Sets configuration properties for a plugin within a specific
 * context.
 * The properties can be provided as command line options (-prop group:key=value) or in a Yaml file.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ConfigurePlugin extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.PLUGIN_NAME,
                                                     CommandOptions.PLUGIN_VERSION,
                                                     CommandOptions.PLUGIN_CONTEXT,
                                                     CommandOptions.TOKEN,
                                                     CommandOptions.PROPERTIES_FILE,
                                                     PROPERTIES);
        String url = createUrl(commandLine, "/v1/plugins/configuration");


        PluginConfiguration pluginConfigurationionConfiguration = new PluginConfiguration(commandLine.getOptionValue(
                CommandOptions.PLUGIN_NAME.getOpt()),
                                                                                          commandLine.getOptionValue(
                                                                                                  CommandOptions.PLUGIN_VERSION
                                                                                                          .getOpt()),
                                                                                          commandLine.getOptionValue(
                                                                                                  CommandOptions.PLUGIN_CONTEXT
                                                                                                          .getOpt()));

        if (commandLine.hasOption(PROPERTIES.getOpt())) {
            for (String metadataProperty : commandLine.getOptionValues(PROPERTIES.getOpt())) {
                String[] keyValue = metadataProperty.split("=", 2);
                if (keyValue.length != 2) {
                    throw new IllegalArgumentException("Property value must be group:key=value - " + keyValue);
                }
                String[] keyParts = keyValue[0].split(":", 2);
                if (keyParts.length != 2) {
                    throw new IllegalArgumentException("Property value must be group:key=value - " + keyValue);
                }
                pluginConfigurationionConfiguration.addProperty(keyParts[0].trim(),
                                                                keyParts[1].trim(),
                                                                keyValue[1].trim());
            }
        }
        if (commandLine.hasOption(CommandOptions.PROPERTIES_FILE.getOpt())) {
            Yaml yaml = new Yaml();
            try (InputStream inputStream = new FileInputStream(option(commandLine, CommandOptions.PROPERTIES_FILE))) {
                Map<String, Map<String, Object>> obj = yaml.load(inputStream);
                pluginConfigurationionConfiguration.setProperties(obj);
            }
        }

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            postJSON(httpclient, url, pluginConfigurationionConfiguration, 200, getToken(commandLine));
        }
    }
}
