package io.axoniq.sample;

import io.axoniq.axonhub.client.command.CommandPriorityCalculator;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.correlation.CorrelationDataProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.lang.management.ManagementFactory;

/**
 * Author: marc
 */
@SpringBootApplication
public class CommandApplication {

    public static void main(String[] args) {
        SpringApplication.run(CommandApplication.class, args);
    }


    @Bean
    public TokenStore tokenStore() {
        return new InMemoryTokenStore();
    }

    @Bean
    public CommandPriorityCalculator commandPriorityCalculator() {
        return new CommandPriorityCalculator() {
            @Override
            public int determinePriority(CommandMessage<?> command) {
                if( command.getCommandName().toLowerCase().contains("batch")) return 0;
                return 10;
            }
        };
    }

    @Bean
    CorrelationDataProvider correlationDataProvider() {
        return message -> {
            String nodeId = ManagementFactory.getRuntimeMXBean().getName();
            return message.getMetaData().and("nodeId", nodeId);
        };
    }
}
