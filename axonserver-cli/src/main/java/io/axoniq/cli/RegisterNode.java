package io.axoniq.cli;

import io.axoniq.cli.json.ClusterNode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static io.axoniq.cli.CommandOptions.INTERNALHOST;
import static io.axoniq.cli.CommandOptions.INTERNALPORT;

/**
 * @author Marc Gathier
 */
public class RegisterNode extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, INTERNALHOST, CommandOptions.INTERNALPORT, CommandOptions.CONTEXTS,
                                                     CommandOptions.TOKEN);

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

        Map<String, ClusterNode.ContextRoleJSON> contextMap = new HashMap<>();
        if( commandLine.hasOption(CommandOptions.CONTEXTS.getOpt()) ) {
            String[] storageContexts = commandLine.getOptionValues(CommandOptions.CONTEXTS.getOpt());
            for (String storageContext : storageContexts) {
                contextMap.put(storageContext, new ClusterNode.ContextRoleJSON(storageContext, true, true));
            }
        }

        if(! contextMap.isEmpty()) {
            clusterNode.setContexts(new ArrayList<>(contextMap.values()));
        }


        try (CloseableHttpClient httpclient = createClient(commandLine) ) {
            postJSON(httpclient, url, clusterNode, 200, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
        }
    }
}
