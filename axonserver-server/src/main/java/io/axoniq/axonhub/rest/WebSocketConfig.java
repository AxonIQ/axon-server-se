package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.licensing.Limits;
import io.axoniq.axonhub.message.command.CommandMetricsWebSocket;
import io.axoniq.axonhub.message.query.QueryMetricsWebSocket;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.web.socket.config.annotation.AbstractWebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;

/**
 * Author: marc
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig extends AbstractWebSocketMessageBrokerConfigurer {
    private final Limits limits;

    public WebSocketConfig(Limits limits) {
        this.limits = limits;
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        config.enableSimpleBroker("/topic");
        config.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/axonhub-platform-websocket").withSockJS();
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(new ChannelInterceptorAdapter() {
            @Override
            public Message<?> preSend(Message<?> message, MessageChannel channel) {
                StompHeaderAccessor headerAccessor= StompHeaderAccessor.wrap(message);
                if(CommandMetricsWebSocket.DESTINATION.equals(headerAccessor.getDestination()) && ! Feature.BASIC_APP_MONITORING.enabled(limits))
                    throw new MessagingException("Access to topic denied");
                if(QueryMetricsWebSocket.DESTINATION.equals(headerAccessor.getDestination()) && ! Feature.BASIC_APP_MONITORING.enabled(limits))
                    throw new MessagingException("Access to topic denied");

                return message;
            }
        });
    }
}
