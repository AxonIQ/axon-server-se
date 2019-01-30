package io.axoniq.cli;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Marc Gathier
 */
public class Metrics extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args

        CommandLine commandLine = processCommandLine( args[0], args, CommandOptions.TOKEN);

        String url = createUrl(commandLine, "/actuator/metrics");

        try (CloseableHttpClient httpclient  = createClient(commandLine)) {
            Map<String, Long> metricValues = new ConcurrentSkipListMap<>();
            Map<String, String[]> metrics = getMap(httpclient, url, new TypeReference<Map<String, String[]>>(){}, 200, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
            String[] names = metrics.get("names");
            Arrays.stream(names).parallel().filter(n -> n.startsWith("axon")).forEach(n -> {
                   String metricUrl = url + "/" + n;
                try {
                    Map<String,?> metricsDetails = getMap(httpclient, metricUrl, new TypeReference<Map<String,?>>(){}, 200, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
                    List<Map<String, ?>> measurements = (List<Map<String, ?>>) metricsDetails.get("measurements");
                    measurements.forEach(m -> {
                        if( m.get("statistic").equals("COUNT")) {
                            metricValues.put( n, ((Double)m.get("value")).longValue());
                        }
                        if( m.get("statistic").equals("VALUE")) {
                            metricValues.put( n, ((Double)m.get("value")).longValue());
                        }
                    });

                } catch (IOException | RuntimeException e) {
                    System.err.println(e.getMessage());
                }
            });

            metricValues.forEach((name,value)->System.out.printf("%-50s %10s\n", name, value));

        }
    }



}
