/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.function.Predicate;


/**
 * @author Marc Gathier
 */
public class AxonIQCliCommand {

    protected static CommandLine processCommandLine(String name, String[] args, Option... options) {
        Options cliOptions = new Options()
                .addOption(CommandOptions.ADDRESS)
                .addOption(CommandOptions.USE_HTTPS)
                .addOption(CommandOptions.CONNECT_INSECURE)
                .addOption(CommandOptions.OUTPUT);
        for (Option option : options) {
            cliOptions.addOption(option);
        }

        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(cliOptions, args);
        } catch (ParseException ex) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(name, cliOptions);
            throw new RuntimeException("Invalid command", ex);
        }
    }

    protected static String createUrl(CommandLine commandLine, String uri, Option... args) {
        String address = commandLine.getOptionValue(CommandOptions.ADDRESS.getOpt());
        if (address == null) {
            address = commandLine.hasOption(CommandOptions.USE_HTTPS.getOpt())
                    ? "https://localhost:8024" : "http://localhost:8024";
        }
        StringBuilder builder = new StringBuilder();
        builder.append(address).append(uri);

        for (Option c : args) {
            builder.append("/").append(commandLine.getOptionValue(c.getOpt()));
        }
        return builder.toString();
    }

    protected static CloseableHttpClient createClient(CommandLine commandLine) {
        try {
            String address = commandLine.getOptionValue(CommandOptions.ADDRESS.getOpt());
            if (address == null) {
                address = commandLine.hasOption(CommandOptions.USE_HTTPS.getOpt())
                        ? "https://localhost:8024" : "http://localhost:8024";
            }
            if (address.startsWith("https")) {
                HttpClientBuilder clientBuilder = HttpClients
                        .custom()
                        .useSystemProperties()
                        .disableRedirectHandling();
                if (commandLine.hasOption(CommandOptions.CONNECT_INSECURE.getOpt())) {
                    clientBuilder
                            .setSSLContext(new SSLContextBuilder()
                                                   .loadTrustMaterial(null, (certificate, authType) -> true).build())
                            .setSSLHostnameVerifier(new NoopHostnameVerifier());
                }

                return clientBuilder.build();
            }

            return HttpClientBuilder.create()
                                    .disableRedirectHandling()
                                    .useSystemProperties()
                                    .build();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    protected static <T> T getJSON(CloseableHttpClient httpclient, String url, Class<T> resultClass,
                                   int expectedStatusCode, String token) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Accept", "application/json");
        if (token != null) {
            httpGet.addHeader("AxonIQ-Access-Token", token);
        }

        ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                                                                 false);

        CloseableHttpResponse response = httpclient.execute(httpGet);
        if (response.getStatusLine().getStatusCode() != expectedStatusCode) {
            throw new CommandExecutionException(response.getStatusLine().getStatusCode(),
                                                url,
                                                response.getStatusLine().toString() + " - " + responseBody(response));
        }

        if (resultClass.equals(String.class)) {
            return (T) responseBody(response);
        }

        try (InputStream is = response.getEntity().getContent()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            return objectMapper.readValue(reader, resultClass);
        }
    }

    protected static String responseBody(CloseableHttpResponse response) {
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
            builder.append(reader.readLine());
        } catch (IOException e) {
        }
        return builder.toString();
    }

    protected static <T> T getMap(CloseableHttpClient httpclient, String url, TypeReference<T> typeReference,
                                  int expectedStatusCode, String token) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        if (token != null) {
            httpGet.addHeader("AxonIQ-Access-Token", token);
        }
        httpGet.addHeader("Accept", "application/json");


        CloseableHttpResponse response = httpclient.execute(httpGet);
        if (response.getStatusLine().getStatusCode() != expectedStatusCode) {
            throw new CommandExecutionException(response.getStatusLine().getStatusCode(),
                                                url,
                                                response.getStatusLine().toString() + " - " + responseBody(response));
        }

        try (InputStream is = response.getEntity().getContent()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(reader, typeReference);
        }
    }

    protected static void delete(CloseableHttpClient httpclient, String url, int expectedStatusCode, String token)
            throws IOException {
        delete(httpclient, url, statusCode -> statusCode == expectedStatusCode, token);
    }

    protected static void delete(CloseableHttpClient httpclient, String url, Predicate<Integer> statusCodeCheck,
                                 String token)
            throws IOException {
        HttpDelete httpDelete = new HttpDelete(url);
        if (token != null) {
            httpDelete.addHeader("AxonIQ-Access-Token", token);
        }

        CloseableHttpResponse response = httpclient.execute(httpDelete);
        if (!statusCodeCheck.test(response.getStatusLine().getStatusCode())) {
            throw new CommandExecutionException(response.getStatusLine().getStatusCode(),
                                                url,
                                                response.getStatusLine().toString() + " - " + responseBody(response));
        }
    }

    protected static String postJSON(CloseableHttpClient httpclient, String url, Object value, int expectedStatusCode,
                                     String token) throws IOException {
        return postJSON(httpclient, url, value, expectedStatusCode, token, String.class);
    }

    protected static <T> T postJSON(CloseableHttpClient httpclient, String url, Object value, int expectedStatusCode,
                                    String token, Class<T> responseClass) throws IOException {
        return postJSON(httpclient, url, value, i -> i == expectedStatusCode, token, responseClass);
    }

    protected static <T> T postJSON(CloseableHttpClient httpclient, String url, Object value,
                                    Predicate<Integer> statusCodeCheck,
                                    String token, Class<T> responseClass)
            throws IOException {
        HttpPost httpPost = new HttpPost(url);
        if (token != null) {
            httpPost.addHeader("AxonIQ-Access-Token", token);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        if (value != null) {
            httpPost.addHeader("Content-Type", "application/json");
            HttpEntity entity = new ByteArrayEntity(objectMapper.writeValueAsBytes(value));
            httpPost.setEntity(entity);
        }

        CloseableHttpResponse response = httpclient.execute(httpPost);
        if (!statusCodeCheck.test(response.getStatusLine().getStatusCode())) {
            throw new CommandExecutionException(response.getStatusLine().getStatusCode(), url,
                                                response.getStatusLine().toString() + " - "
                                                        + responseBody(response));
        }

        if (responseClass.equals(String.class)) {
            return (T) responseBody(response);
        }

        return objectMapper.readValue(response.getEntity().getContent(), responseClass);
    }

    protected static <T> T patchJSON(CloseableHttpClient httpclient, String url, Object value, int expectedStatusCode,
                                    String token, Class<T> responseClass)
            throws IOException {
        HttpPatch httpPost = new HttpPatch(url);
        if (token != null) {
            httpPost.addHeader("AxonIQ-Access-Token", token);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        if (value != null) {
            httpPost.addHeader("Content-Type", "application/json");
            HttpEntity entity = new ByteArrayEntity(objectMapper.writeValueAsBytes(value));
            httpPost.setEntity(entity);
        }

        CloseableHttpResponse response = httpclient.execute(httpPost);
        if (response.getStatusLine().getStatusCode() != expectedStatusCode) {
            throw new CommandExecutionException(response.getStatusLine().getStatusCode(), url,
                                                response.getStatusLine().toString() + " - "
                                                        + responseBody(response));
        }

        if (responseClass.equals(String.class)) {
            return (T) responseBody(response);
        }

        return objectMapper.readValue(response.getEntity().getContent(), responseClass);
    }

    public static String option(CommandLine commandLine, Option option) {
        return commandLine.getOptionValue(option.getOpt());
    }

    public static boolean jsonOutput(CommandLine commandLine) {
        return "json".equals(option(commandLine, CommandOptions.OUTPUT));
    }

    protected static String getToken(CommandLine commandLine) {
        String token = commandLine.getOptionValue(CommandOptions.TOKEN.getOpt());
        if (token == null) {
            File tokenFile = new File("security/.token");
            if (tokenFile.exists() && tokenFile.canRead()) {
                try (BufferedReader reader = new BufferedReader(new FileReader(tokenFile))) {
                    token = reader.readLine();
                } catch (IOException e) {
                    System.err.println("Cannot read token file: " + e.getMessage());
                }
            }
        }
        return token;
    }
}
