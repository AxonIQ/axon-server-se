package io.axoniq.sample;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.PlatformConnectionManager;
import org.axonframework.axonserver.connector.event.axon.AxonServerEventStore;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.serialization.Serializer;
import org.axonframework.spring.config.AxonConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Author: marc
 */
@SpringBootApplication
public class MyApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }

    @Bean
    public EventStore eventStore(AxonServerConfiguration axonServerConfiguration,
                                 AxonConfiguration configuration,
                                 PlatformConnectionManager platformConnectionManager,
                                 Serializer snapshotSerializer,
                                 @Qualifier("eventSerializer") Serializer eventSerializer) {
        return AxonServerEventStore.builder()
                                   .configuration(axonServerConfiguration)
                                   .snapshotFilter(snapshotData -> (snapshotData.getSequenceNumber() % 3) == 0)
                                   .platformConnectionManager(platformConnectionManager)
                                   .snapshotSerializer(snapshotSerializer)
                                   .eventSerializer(eventSerializer)
                                   .upcasterChain(configuration.upcasterChain())
                                   .build();
    }


}
