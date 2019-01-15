package io.axoniq.axonserver;

import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PreDestroy;

/**
 * Author: marc
 */
@SpringBootApplication
@EnableAsync
@EnableScheduling
public class AxonServer {
    public static void main(String[] args) {
        System.setProperty("spring.config.name", "axonserver");
        SpringApplication.run(AxonServer.class, args);
    }

    @PreDestroy
    public void clean() {
        GrpcFlowControlledDispatcherListener.shutdown();

    }

}
