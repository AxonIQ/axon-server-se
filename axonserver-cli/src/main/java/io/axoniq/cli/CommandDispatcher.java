package io.axoniq.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public class CommandDispatcher {
    @FunctionalInterface
    interface CommandProcessor {
        void process(String[] strings) throws Exception;
    }

    private static final Map<String, CommandProcessor> executorMap = new HashMap<>();
    static  {
        executorMap.put("cluster", Cluster::run);
        executorMap.put("delete-application", DeleteApplication::run);
        executorMap.put("register-application", RegisterApplication::run);
        executorMap.put("applications", ListApplications::run);
        executorMap.put("register-node", RegisterNode::run);
        executorMap.put("unregister-node", UnregisterNode::run);
        executorMap.put("register-context", RegisterContext::run);
        executorMap.put("delete-context", DeleteContext::run);
        executorMap.put("contexts", ListContexts::run);
        executorMap.put("register-user", RegisterUser::run);
        executorMap.put("delete-user", DeleteUser::run);
        executorMap.put("users", ListUsers::run);
        executorMap.put("metrics", Metrics::run);
    }

    public static void main(String[] args)  {
        if( args.length == 0) {
            System.err.println("No command specified. Valid commands: ");
            executorMap.keySet().forEach(System.err::println);

            System.exit(1);
        }

        CommandProcessor executor = executorMap.get(args[0]);
        if( executor != null) {
            try {
                executor.process(args);
            } catch (CommandExecutionException ex) {
                System.err.println("Error processing command '" + args[0] + "' on '"+ ex.getUrl() + "': " + ex.getMessage());
                System.exit(ex.getErrorCode());
            } catch (Exception ex) {
                System.err.println("Error processing command '" + args[0] + "': " + ex.getMessage());
                System.exit(1);
            }
        } else {
            System.err.println("Invalid command specified: " + args[0] +". Valid commands: ");
            executorMap.keySet().forEach(System.err::println);
            System.exit(1);
        }
    }
}
