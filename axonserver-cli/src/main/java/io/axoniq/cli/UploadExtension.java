/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.File;
import java.io.IOException;

/**
 * CLI handler for the upload-extension command. Adds an extension to Axon Server. Returns the extension name and
 * version as defined inside the extension file.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class UploadExtension extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0],
                                                     args,
                                                     CommandOptions.EXTENSION_FILE,
                                                     CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/extensions");
        File file = new File(commandLine.getOptionValue(CommandOptions.EXTENSION_FILE.getOpt()));
        if (!file.exists() || !file.canRead()) {
            throw new CommandExecutionException(404, url, file.getAbsolutePath() + ": File does not exists");
        }

        String token = getToken(commandLine);
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            HttpPost uploadFile = new HttpPost(url);
            if (token != null) {
                uploadFile.addHeader("AxonIQ-Access-Token", token);
            }
            MultipartEntityBuilder builder = MultipartEntityBuilder.create();

            builder.addBinaryBody(
                    "bundle", file,
                    ContentType.APPLICATION_OCTET_STREAM,
                    file.getName()
            );

            HttpEntity multipart = builder.build();
            uploadFile.setEntity(multipart);
            CloseableHttpResponse response = httpclient.execute(uploadFile);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new CommandExecutionException(response.getStatusLine().getStatusCode(), url,
                                                    response.getStatusLine().toString() + " - "
                                                            + responseBody(response));
            }

            System.out.println(responseBody(response));
        }
    }
}
