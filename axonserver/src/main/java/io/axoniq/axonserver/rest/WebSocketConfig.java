/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * @author Marc Gathier
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final boolean setWebSocketAllowedOrigins;
    private final String webSocketAllowedOrigins;

    public WebSocketConfig(MessagingPlatformConfiguration config) {
        this.setWebSocketAllowedOrigins = config.isSetWebSocketAllowedOrigins();
        this.webSocketAllowedOrigins = config.getWebSocketAllowedOrigins();
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        config.enableSimpleBroker("/topic");
        config.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        if (setWebSocketAllowedOrigins) {
            registry.addEndpoint("/axonserver-platform-websocket")
                    .setAllowedOriginPatterns(webSocketAllowedOrigins)
                    .withSockJS();
        } else {
            registry.addEndpoint("/axonserver-platform-websocket")
                    .withSockJS();
        }
    }
}
