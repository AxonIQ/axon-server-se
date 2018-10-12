package net.axoniq.axonserver.connectors.rabbitmq;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: marc
 */
@Configuration
public class Config {

    private final ConnectionFactory connectionFactory;
    final static String queueName = "spring-boot";

    public Config(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }



    @Bean
    Queue queue() {
        return new Queue(queueName, false);
    }

    @Bean
    Queue debugQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-message-ttl", 60000);
        return new Queue("Debug", true, false, false, args);
    }

    @Bean
    TopicExchange exchange() {
        return new TopicExchange("spring-boot-exchange");
    }

    @Bean
    Binding binding(Queue queue, TopicExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("#");
    }

    ConnectionFactory connectionFactory() {
        return connectionFactory;
//        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host, port);
//        connectionFactory.setUsername(username);
//        connectionFactory.setPassword(password);
//        connectionFactory.setVirtualHost(virtualHost);
//        return connectionFactory;
    }

    @Bean
    Binding debugBinding(Queue debugQueue, TopicExchange exchange) {
        return BindingBuilder.bind(debugQueue).to(exchange).with("#");
    }
}
