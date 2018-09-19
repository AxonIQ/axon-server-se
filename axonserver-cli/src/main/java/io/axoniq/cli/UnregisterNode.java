package io.axoniq.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * Author: marc
 */
public class UnregisterNode extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args,
                CommandOptions.NODENAME, CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/cluster", CommandOptions.NODENAME);
        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            delete(httpclient, url,  200, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
        }
    }
}
