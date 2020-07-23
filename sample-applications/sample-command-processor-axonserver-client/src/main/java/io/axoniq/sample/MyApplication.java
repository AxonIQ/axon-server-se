package io.axoniq.sample;

import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.scheduling.EventScheduler;
import org.axonframework.eventhandling.scheduling.java.SimpleEventScheduler;
import org.axonframework.springboot.util.jpa.ContainerManagedEntityManagerProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.Executors;

/**
 * @author Marc Gathier
 */
@SpringBootApplication
public class MyApplication {

    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }

    @Bean
    public EntityManagerProvider entityManagerProvider() {
        return new ContainerManagedEntityManagerProvider();
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
//@Bean
//public AxonServerEventScheduler eventScheduler(AxonServerConfiguration axonServerConfiguration,
//                                               AxonServerConnectionManager axonServerConnectionManager,
//                                               @Qualifier("eventSerializer") Serializer eventSerializer) {
//    return AxonServerEventScheduler.builder()
//                                   .eventSerializer(eventSerializer)
//                                   .configuration(axonServerConfiguration)
//                                   .connectionManager(axonServerConnectionManager)
//                                   .build();
//}
    @Bean
    public EventScheduler eventScheduler(EventBus eventBus) {
        return SimpleEventScheduler.builder()
                                   .eventBus(eventBus)
                                   .scheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
                                   .build();
    }
}
