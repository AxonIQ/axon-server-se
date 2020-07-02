package io.axoniq.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

import static io.axoniq.cli.CommandOptions.*;

/**
 * @author Marc Gathier
 */
public class DeleteNodeFromReplicationGroup extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0],
                                                     args,
                                                     REPLICATIONGROUP,
                                                     NODE_NAME,
                                                     PRESERVE_EVENT_STORE,
                                                     CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/replicationgroups", REPLICATIONGROUP, NODE_NAME);
        if (commandLine.hasOption(PRESERVE_EVENT_STORE.getLongOpt())) {
            url += "?preserveEventStore=true";
        }
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            delete(httpclient, url, 202, getToken(commandLine));
        }
    }
}
