package io.axoniq.cli;

import io.axoniq.cli.json.ClusterNode;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.util.Arrays;
import java.util.Comparator;

/**
 * @author Marc Gathier
 */
public class Cluster extends AxonIQCliCommand {
    public static void run(String[] args)  {
        // check args

        CommandLine commandLine = processCommandLine( args[0], args, CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/v1/public");

        try (CloseableHttpClient httpclient  = createClient(commandLine)) {
            ClusterNode[] nodes = getJSON(httpclient, url, ClusterNode[].class, 200, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
            System.out.printf("%-40s %-40s %10s %10s %s\n", "Name", "Hostname", "gRPC Port", "HTTP Port", "Connected");
            Arrays.stream(nodes).sorted(Comparator.comparing(ClusterNode::getName)).forEach(node -> {
                System.out.printf("%-40s %-40s %10d %10d %s\n", node.getName(), node.getHostName(), node.getGrpcPort(), node.getHttpPort(), node.isConnected() ? "*" : "");
            });

        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(url, e);
        }
    }


}
