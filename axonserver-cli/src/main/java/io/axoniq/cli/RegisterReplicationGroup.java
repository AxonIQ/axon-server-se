package io.axoniq.cli;

import io.axoniq.cli.json.ReplicationGroupJSON;
import io.axoniq.cli.json.RestResponse;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.axoniq.cli.CommandOptions.*;

/**
 * @author Marc Gathier
 */
public class RegisterReplicationGroup extends AxonIQCliCommand {

    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0],
                                                     args,
                                                     REPLICATIONGROUP,
                                                     PRIMARY_NODES,
                                                     ACTIVE_BACKUP_NODES,
                                                     PASSIVE_BACKUP_NODES,
                                                     MESSAGING_ONLY_NODES,
                                                     SECONDARY_NODES,
                                                     CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/replicationgroups");

        ReplicationGroupJSON replicationGroup = new ReplicationGroupJSON();
        replicationGroup.setName(commandLine.getOptionValue(REPLICATIONGROUP.getOpt()));
        List<ReplicationGroupJSON.NodeAndRole> nodeRolesMap = new ArrayList<>();
        Set<String> definedNodes = new HashSet<>();
        addNodes(commandLine, PRIMARY_NODES, "PRIMARY", definedNodes, nodeRolesMap);
        addNodes(commandLine, SECONDARY_NODES, "SECONDARY", definedNodes, nodeRolesMap);
        addNodes(commandLine, ACTIVE_BACKUP_NODES, "ACTIVE_BACKUP", definedNodes, nodeRolesMap);
        addNodes(commandLine, PASSIVE_BACKUP_NODES, "PASSIVE_BACKUP", definedNodes, nodeRolesMap);
        addNodes(commandLine, MESSAGING_ONLY_NODES, "MESSAGING_ONLY", definedNodes, nodeRolesMap);

        replicationGroup.setRoles(nodeRolesMap);

        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            postJSON(httpclient, url, replicationGroup, 200, getToken(commandLine),
                     RestResponse.class);
        }
    }

    private static void addNodes(CommandLine commandLine, Option nodes, String role, Set<String> definedNodes,
                                 List<ReplicationGroupJSON.NodeAndRole> nodeRolesMap) {
        if (commandLine.hasOption(nodes.getOpt())) {
            for (String primary : commandLine.getOptionValues(nodes.getOpt())) {
                if (definedNodes.contains(primary)) {
                    throw new IllegalArgumentException("Node can only be provided once");
                }
                nodeRolesMap.add(new ReplicationGroupJSON.NodeAndRole(primary, role));
                definedNodes.add(primary);
            }
        }
    }
}
