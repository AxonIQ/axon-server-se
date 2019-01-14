package net.axoniq.axonhub.connectors.kafka;

import io.axoniq.axonserver.connector.Event;
import io.axoniq.axonserver.connector.EventConnector;
import io.axoniq.axonserver.connector.UnitOfWork;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.time.temporal.ChronoField.*;

/**
 * @author Marc Gathier
 */
@Import(KafkaEventConnector.Config.class)
public class KafkaEventConnector implements EventConnector {
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
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Override
    public UnitOfWork createUnitOfWork() {
        return new KafkaUnitOfWork();
    }

    @Override
    public void publish(Event event) {
        Map<String, Object> headers = event.getMetaData();
        Message<byte[]> message = new GenericMessage<>(event.getPayload(), headers);
        kafkaTemplate.send( message);
        kafkaTemplate.flush();
    }


    @Configuration
    @EnableKafka
    public static class Config {
        @Value("${axoniq.axonserver.kafka.bootstrapServers:172.17.0.4:9092}")
        private String bootstrapServers;
        @Value("${axoniq.axonserver.kafka.retries:1}")
        private int retries;
        @Value("${axoniq.axonserver.kafka.batchSize:16384}")
        private int batchSize;
        @Value("${axoniq.axonserver.kafka.lingerMs:1}")
        private int lingerMs;
        @Value("${axoniq.axonserver.kafka.bufferMemory:33554432}")
        private long bufferMemory;
        @Value("${axoniq.axonserver.kafka.transactionIdPrefix:axonserver}")
        private String transactionIdPrefix;
        @Value("${axoniq.axonserver.kafka.defaultTopic:test}")
        private String defaultTopic;

        @Bean
        public ProducerFactory<String, byte[]> producerFactory() {
            return new DefaultKafkaProducerFactory<>(producerConfigs());
        }

        @Bean
        public Map<String, Object> producerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            props.put(ProducerConfig.RETRIES_CONFIG, retries);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
            props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            return props;
        }

        @Bean
        public KafkaTemplate<String, byte[]> kafkaTemplate() {
            KafkaTemplate<String, byte[]> template = new KafkaTemplate<>(producerFactory());
            template.setDefaultTopic("test");
            return template;
        }

    }

    private class KafkaUnitOfWork implements UnitOfWork {
        private List<Event> events = new ArrayList<>();

        @Override
        public void publish(Event event) {
            events.add(event);
        }

        @Override
        public void commit() {
            List<ListenableFuture<SendResult<String, byte[]>>> futures = new ArrayList<>();
            events.forEach(e -> futures.add(kafkaTemplate.send( createMessage(e))));

            futures.forEach(f -> {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e.getCause());
                }
            });

            kafkaTemplate.flush();
        }

        private Message<byte[]> createMessage(Event event) {
            Map<String, Object> headers = new HashMap<>();
            headers.put("kafka_messageKey", event.getIdentifier());
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
//            String topic = (String)headers.get("kafka_topic", String.class);
//            Integer partition = (Integer)headers.get("kafka_partitionId", Integer.class);

            return new GenericMessage<>(event.getPayload(), headers);
        }

        @Override
        public void rollback() {
            // no-op as nothing done yet
        }
        private String formatInstant(long timestamp) {
            return ISO_UTC_DATE_TIME.format(Instant.ofEpochMilli(timestamp));
        }

    }
}
