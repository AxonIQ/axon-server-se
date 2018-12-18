package io.axoniq.cli;

import io.axoniq.cli.json.RestResponse;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * Author: marc
 */
public class InitNode extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        CommandLine commandLine = processCommandLine(args[0], args);
        String url = createUrl(commandLine, "/v1/context/init");

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            getJSON(httpclient, url, RestResponse.class, 200, null);
        }
    }
}
