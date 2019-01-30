package net.axoniq.axonserver.connectors.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import io.axoniq.axonserver.connector.ConnectorEvent;
import io.axoniq.axonserver.connector.EventConnector;
import io.axoniq.axonserver.connector.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static java.time.temporal.ChronoField.*;

/**
 * @author Marc Gathier
 */
@Import(Config.class)
public class RabbitMQEventConnector implements EventConnector {
    private static final DateTimeFormatter ISO_UTC_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .appendFraction(NANO_OF_SECOND, 3, 9, true)
            .appendOffsetId()
            .toFormatter()
            .withZone(ZoneOffset.UTC);

    @Autowired
    private Config config;
    private final Logger logger = LoggerFactory.getLogger(RabbitMQEventConnector.class);


    @Override
    public UnitOfWork createUnitOfWork() {
        logger.info( "Start unit of work to: {}", config.connectionFactory());
        Channel channel = config.connectionFactory().createConnection().createChannel(true);
        try {
            channel.txSelect();
            return new UnitOfWork() {
                @Override
                public void publish(List<? extends ConnectorEvent> events) {
                    events.forEach(event -> {
                        try {

                            sendMessage(channel, createMessage(event));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                @Override
                public void commit() {
                    try {
                        channel.txCommit();
                    } catch (IOException e) {
                        throw new RuntimeException("Commit failed", e);
                    }

                }

                @Override
                public void rollback() {
                    try {
                        channel.txRollback();
                    } catch (IOException e) {
                        throw new RuntimeException("Rollback failed", e);
                    }

                }
            };
        } catch( IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            tryClose(channel);
        }
    }

    private void sendMessage(Channel channel, AMQPMessage message) throws IOException {
        channel.basicPublish("spring-boot-exchange", message.getRoutingKey(), message.isMandatory(),
                message.isImmediate(), message.getProperties(), message.getBody());

    }

    private AMQPMessage createMessage(ConnectorEvent event) {
        AMQP.BasicProperties.Builder properties = new AMQP.BasicProperties.Builder();
        Map<String, Object> headers = new HashMap<>();
        event.getMetaData().forEach((k, v) -> headers.put("axon-metadata-" + k, v));
        headers.put("axon-message-id", event.getIdentifier());
        headers.put("axon-message-type", event.getPayloadType());
        headers.put("axon-message-revision", event.getPayloadRevision());
        headers.put("axon-message-timestamp", formatInstant(event.getTimestamp()));
        if (event.isDomainEvent()) {
            headers.put("axon-message-aggregate-id", event.getAggregateIdentifier());
            headers.put("axon-message-aggregate-seq", event.getSequenceNumber());
            headers.put("axon-message-aggregate-type", event.getType());
        }
        properties.headers(headers);
//        if (durable) {
//            properties.deliveryMode(2);
//        }
        return new AMQPMessage(event.getPayload(), event.getAggregateType(), properties.build(), false, false);

    }

    private String formatInstant(long timestamp) {
        return ISO_UTC_DATE_TIME.format(Instant.ofEpochMilli(timestamp));
    }

    @Override
    public void publish(ConnectorEvent event) {
        Channel channel = config.connectionFactory().createConnection().createChannel(false);
        try {
            sendMessage(channel, createMessage(event));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            tryClose(channel);
        }
    }

    private void tryClose(Channel channel) {
        try {
            channel.close();
        } catch (IOException | TimeoutException e) {
            logger.info("Unable to close channel. It might already be closed.", e);
        }
    }


}
