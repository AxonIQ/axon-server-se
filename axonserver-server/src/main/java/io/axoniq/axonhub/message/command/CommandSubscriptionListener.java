package io.axoniq.axonhub.message.command;

/**
 * Author: marc
 */
public interface CommandSubscriptionListener {
    void unsubscribeCommand(String context, String command, String client, String componentName);
    void subscribeCommand(String context, String command, String client, String componentName);

}
