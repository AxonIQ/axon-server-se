package io.axoniq.cli;

import io.axoniq.cli.json.User;
import org.apache.commons.cli.CommandLine;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * Author: marc
 */
public class RegisterUser extends AxonIQCliCommand {
    public static void run(String[] args) throws IOException {
        // check args
        CommandLine commandLine = processCommandLine(args[0], args, CommandOptions.USERNAME,
                CommandOptions.PASSWORD,
                CommandOptions.ROLES,
                CommandOptions.TOKEN);
        String url = createUrl(commandLine, "/v1/users");

        String password = commandLine.getOptionValue(CommandOptions.PASSWORD.getOpt());
        if( password == null) {
            password = PasswordReader.INSTANCE.readPassword();
        }

        User user= new User(commandLine.getOptionValue(CommandOptions.USERNAME.getOpt()),
                password, commandLine.getOptionValues(CommandOptions.ROLES.getOpt()));

        // get http client
        try (CloseableHttpClient httpclient = createClient(commandLine)) {
            postJSON(httpclient, url, user, 200, commandLine.getOptionValue(CommandOptions.TOKEN.getOpt()));
        }
    }
}
