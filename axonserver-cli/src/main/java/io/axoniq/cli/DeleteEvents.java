package io.axoniq.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

import static io.axoniq.cli.AxonIQCliCommand.*;

/**
 * @author Greg Woods
 * @since 4.2
 */
public class DeleteEvents{
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/devmode/purge-events");
        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            delete(httpclient, url, 200, getToken(commandLine));
        }
    }
}
