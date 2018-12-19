package io.axoniq.cli;

import io.axoniq.cli.json.ClusterNode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.axoniq.cli.CommandOptions.INTERNALHOST;
import static io.axoniq.cli.CommandOptions.INTERNALPORT;

/**
 * Author: marc
 */
public class RegisterNode extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, INTERNALHOST, CommandOptions.INTERNALPORT, CommandOptions.CONTEXTS, CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/cluster");
        Number port = null;
        try {
            port = (Number)commandLine.getParsedOptionValue(INTERNALPORT.getOpt());
        } catch (ParseException e) {
            throw new RuntimeException("Invalid value for option " + INTERNALPORT);
        }
        if( port == null) port = 8224;

        ClusterNode clusterNode = new ClusterNode(commandLine.getOptionValue(INTERNALHOST.getOpt().charAt(0)),
                port.intValue());

        if( commandLine.hasOption(CommandOptions.CONTEXTS.getOpt())) {
            List<String> contexts = new ArrayList<>(Arrays.asList(commandLine.getOptionValues(CommandOptions.CONTEXTS.getOpt())));
            if(! contexts.isEmpty()) {
                clusterNode.setContexts(contexts);
            }
        }


        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            postJSON(httpclient, url, clusterNode, 200, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
        }
    }
}
