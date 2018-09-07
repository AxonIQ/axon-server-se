package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.message.SubscriptionKey;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;
import org.springframework.web.socket.messaging.SessionUnsubscribeEvent;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Stream;

/**
 * Created by Sara Pellegrini on 18/04/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class CommandMetricsWebSocket {
    public static final String DESTINATION = "/topic/commands";
    private final Set<SubscriptionKey> subscriptions = new CopyOnWriteArraySet<>();
    private final CommandMetricsRegistry commandMetricsRegistry;

    private final CommandRegistrationCache commandRegistrationCache;

    private final SimpMessagingTemplate webSocket;

    public CommandMetricsWebSocket(CommandMetricsRegistry commandMetricsRegistry,
                                   CommandRegistrationCache commandRegistrationCache,
                                   SimpMessagingTemplate webSocket) {
        this.commandMetricsRegistry = commandMetricsRegistry;
        this.commandRegistrationCache = commandRegistrationCache;
        this.webSocket = webSocket;
    }

    @Scheduled(initialDelayString = "10000", fixedRateString = "1000")
    public void publish() {
        commandRegistrationCache.getAll().forEach(
                (commandHandler, registrations) -> getMetrics(commandHandler, registrations).forEach(
                        commandMetric -> webSocket.convertAndSend(DESTINATION, commandMetric)
                ));
    }

    @EventListener
    public void on(SessionSubscribeEvent event) {
        StompHeaderAccessor sha = StompHeaderAccessor.wrap(event.getMessage());
        if( DESTINATION.equals(sha.getDestination())) {
            subscriptions.add( new SubscriptionKey(sha));
        }
    }

    @EventListener
    public void on(SessionUnsubscribeEvent event) {
        StompHeaderAccessor sha = StompHeaderAccessor.wrap(event.getMessage());
        subscriptions.remove(new SubscriptionKey(sha));
    }

    private Stream<CommandMetricsRegistry.CommandMetric> getMetrics(CommandHandler commandHander,
                                                                    Set<CommandRegistrationCache.RegistrationEntry> registrations) {
        return registrations.stream()
                            .map(registration -> commandMetricsRegistry.commandMetric(registration.getCommand(),
                                                                                      commandHander.getClient(),
                                                                                      commandHander.getComponentName()));
    }
}
