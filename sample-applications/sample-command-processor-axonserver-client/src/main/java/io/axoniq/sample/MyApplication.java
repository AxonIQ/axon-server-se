package io.axoniq.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Author: marc
 */
@SpringBootApplication
public class MyApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }

//    @Bean
//    public EventStore eventStore(AxonServerConfiguration axonServerConfiguration,
//                                 AxonConfiguration configuration,
//                                 PlatformConnectionManager platformConnectionManager,
//                                 Serializer snapshotSerializer,
//                                 @Qualifier("eventSerializer") Serializer eventSerializer) {
//        return AxonServerEventStore.builder()
//                                   .configuration(axonServerConfiguration)
//                                   .snapshotFilter(snapshotData -> (snapshotData.getSequenceNumber() % 3) == 0)
//                                   .platformConnectionManager(platformConnectionManager)
//                                   .snapshotSerializer(snapshotSerializer)
//                                   .eventSerializer(eventSerializer)
//                                   .upcasterChain(configuration.upcasterChain())
//                                   .build();
//    }
//

}
