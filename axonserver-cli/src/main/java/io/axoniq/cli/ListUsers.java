package io.axoniq.cli;

import io.axoniq.cli.json.User;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * Author: marc
 */
public class ListUsers extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/public/users");

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            if( jsonOutput(commandLine)) {
                System.out.println(getJSON(httpclient,
                                           url,
                                           String.class,
                                           200,
                                           option(commandLine, CommandOptions.TOKEN)));
            } else {
                User[] users = getJSON(httpclient,
                                       url,
                                       User[].class,
                                       200,
                                       option(commandLine, CommandOptions.TOKEN));
                System.out.printf("%-60s\n", "Name");

                for (User user : users) {
                    System.out.printf("%-60s\n", user.getUserName());
                }
            }
        }


    }
}
