package io.axoniq.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

import static io.axoniq.cli.CommandOptions.CONTEXT;
import static io.axoniq.cli.CommandOptions.NODENAME;

/**
 * Author: marc
 */
public class DeleteNodeFromContext extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CONTEXT, NODENAME, CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/context", CONTEXT, NODENAME);

        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            delete(httpclient, url, 202, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
        }
    }
}
