package io.axoniq.cli;

import io.axoniq.cli.json.ContextNode;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class ListContexts extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/public/context");

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            ContextNode[] contexts = getJSON(httpclient, url, ContextNode[].class, 200, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
            System.out.printf("%-20s %-60s\n", "Name", "Members");

            for( ContextNode context : contexts) {
                System.out.printf("%-20s %-60s\n", context.getContext(),
                        context.getNodes() == null? "" : context.getNodes().stream().map(Object::toString).collect(Collectors.joining(",")));
            }
        }


    }
}
