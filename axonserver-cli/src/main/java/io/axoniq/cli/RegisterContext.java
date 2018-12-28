package io.axoniq.cli;

import io.axoniq.cli.json.ContextNode;
import io.axoniq.cli.json.NodeRoles;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static io.axoniq.cli.CommandOptions.*;

/**
 * Author: marc
 */
public class RegisterContext extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CONTEXT, NODES, CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/context");

        Map<String, NodeRoles> nodeRolesMap = new HashMap<>();
        if( commandLine.hasOption(CommandOptions.NODES.getOpt())) {
            String[] storageNodes = commandLine.getOptionValues(CommandOptions.NODES.getOpt());
            for (String storageNode : storageNodes) {
                nodeRolesMap.put(storageNode, new NodeRoles(storageNode, true, true));
            }
        }

        ContextNode clusterNode = new ContextNode(commandLine.getOptionValue(CONTEXT.getOpt()), new ArrayList<>(nodeRolesMap.values()));

        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            postJSON(httpclient, url, clusterNode, 200, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
        }
    }
}
