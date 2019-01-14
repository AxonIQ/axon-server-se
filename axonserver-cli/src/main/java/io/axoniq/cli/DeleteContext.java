package io.axoniq.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * @author Marc Gathier
 */
public class DeleteContext extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.CONTEXT, CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/context", CommandOptions.CONTEXT);

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            delete(httpclient, url, 200, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
        }
    }
}
