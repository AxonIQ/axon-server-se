package io.axoniq.axonserver.message.command;

/**
 * @author Marc Gathier
 */
public interface CommandSubscriptionListener {
    void unsubscribeCommand(String context, String command, String client, String componentName);
    void subscribeCommand(String context, String command, String client, String componentName);

}
