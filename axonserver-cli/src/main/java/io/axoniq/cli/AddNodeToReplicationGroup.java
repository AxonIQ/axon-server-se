package io.axoniq.cli;

import io.axoniq.cli.json.RestResponse;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

import static io.axoniq.cli.CommandOptions.*;

/**
 * @author Marc Gathier
 */
public class AddNodeToReplicationGroup extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0],
                                                     args,
                                                     REPLICATIONGROUP,
                                                     NODE_NAME,
                                                     NODE_ROLE,
                                                     CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/replicationgroups", REPLICATIONGROUP, NODE_NAME);
        if (commandLine.hasOption(NODE_ROLE.getOpt())) {
            url += "?role=" + commandLine.getOptionValue(NODE_ROLE.getOpt());
        }

        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            RestResponse response = postJSON(httpclient, url, null, 202, getToken(commandLine),
                                             RestResponse.class);
            System.out.println(response.getMessage());
        }
    }
}
