package io.axoniq.cli;

import com.fasterxml.jackson.core.type.TypeReference;
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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;


/**
 * Author: marc
 */
public class AxonIQCliCommand {
    protected static CommandLine processCommandLine(String name, String[] args, Option... options) {
        Options cliOptions = new Options().addOption(CommandOptions.ADDRESS);
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
        if( address == null) address = "http://localhost:8024";
        StringBuilder builder = new StringBuilder();
        builder.append(address).append(uri);

        for (Option c : args) {
            builder.append("/").append(commandLine.getOptionValue(c.getOpt()));
        }
        return builder.toString();
    }

    protected static CloseableHttpClient createClient(CommandLine commandLine)  {
        try {
            String address = commandLine.getOptionValue(CommandOptions.ADDRESS.getOpt());
            if (address == null) address = "http://localhost:8024";
            if (address.startsWith("https")) {
                SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, (certificate, authType) -> true)
                                                               .build();
                return HttpClients.custom()
                                  .disableRedirectHandling()
                                  .setSSLContext(sslContext)
                                  .build();
            }

            return HttpClientBuilder.create().disableRedirectHandling().build();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e ) {
            throw new RuntimeException(e);
        }
    }

    protected static <T> T getJSON(CloseableHttpClient httpclient, String url, Class<T> resultClass, int expectedStatusCode, String token) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Accept", "application/json");
        if (token != null) {
            httpGet.addHeader("AxonIQ-Access-Token", token);
        }

        ObjectMapper objectMapper = new ObjectMapper();

        CloseableHttpResponse response = httpclient.execute(httpGet);
        if (response.getStatusLine().getStatusCode() != expectedStatusCode) {
            throw new CommandExecutionException(response.getStatusLine().getStatusCode(), url, response.getStatusLine().toString() + " - " + responseBody(response));
        }

        try (InputStream is = response.getEntity().getContent()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            return objectMapper.readValue(reader, resultClass);
        }
    }

    private static String responseBody(CloseableHttpResponse response) {
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
            builder.append(reader.readLine());
        } catch (IOException e) {
        }
        return builder.toString();
    }

    protected static <T> T getMap(CloseableHttpClient httpclient, String url, TypeReference<T> typeReference, int expectedStatusCode, String token) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        if (token != null) {
            httpGet.addHeader("AxonIQ-Access-Token", token);
        }
        httpGet.addHeader("Accept", "application/json");

        ObjectMapper objectMapper = new ObjectMapper();

        CloseableHttpResponse response = httpclient.execute(httpGet);
        if (response.getStatusLine().getStatusCode() != expectedStatusCode) {
            throw new CommandExecutionException(response.getStatusLine().getStatusCode(), url, response.getStatusLine().toString() + " - " + responseBody(response));
        }

        try (InputStream is = response.getEntity().getContent()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            return objectMapper.readValue(reader, typeReference);
        }
    }


    protected static void delete(CloseableHttpClient httpclient, String url, int expectedStatusCode, String token) throws IOException {
        HttpDelete httpDelete = new HttpDelete(url);
        if (token != null) {
            httpDelete.addHeader("AxonIQ-Access-Token", token);
        }

        CloseableHttpResponse response = httpclient.execute(httpDelete);
        if (response.getStatusLine().getStatusCode() != expectedStatusCode) {
            throw new CommandExecutionException(response.getStatusLine().getStatusCode(), url,  response.getStatusLine().toString() + " - " + responseBody(response));
        }
    }

    protected static String postJSON(CloseableHttpClient httpclient, String url, Object value, int expectedStatusCode, String token)
            throws IOException {
            HttpPost httpPost = new HttpPost(url);
            if (token != null) {
                httpPost.addHeader("AxonIQ-Access-Token", token);
            }
            if( value != null) {
                ObjectMapper objectMapper = new ObjectMapper();
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

            try (InputStream is = response.getEntity().getContent()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                return reader.readLine();
            }
    }

}
