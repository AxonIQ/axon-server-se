package io.axoniq.axonserver;

import io.axoniq.axonserver.cluster.grpc.LeaderElectionService;
import io.axoniq.axonserver.cluster.grpc.LogReplicationService;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import io.axoniq.axonserver.rest.PluginImportSelector;
import io.axoniq.axonserver.version.VersionInfo;
import io.axoniq.axonserver.version.VersionInfoProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author Marc Gathier
 */
@SpringBootApplication
@EnableAsync
@EnableScheduling
@Import(PluginImportSelector.class)
public class AxonServerEnterprise {

    private static final Logger log = LoggerFactory.getLogger(AxonServerEnterprise.class);
    private final VersionInfoProvider versionInfoProvider;

    public AxonServerEnterprise(VersionInfoProvider versionInfoProvider) {
        this.versionInfoProvider = versionInfoProvider;
    }

    @PostConstruct
    public void versionSet() {
        VersionInfo versionInfo = versionInfoProvider.get();
        if (versionInfo != null) {
            log.info("{} version {}", versionInfo.getProductName(), versionInfo.getVersion());
        }
    }



    public static void main(String[] args) {
        System.setProperty("spring.config.name", "axonserver");
        SpringApplication.run(AxonServerEnterprise.class, args);
    }

    @Bean
    public LogReplicationService logReplicationService(GrpcRaftController grpcRaftController) {
        return new LogReplicationService(grpcRaftController);
    }

    @Bean
    public LeaderElectionService leaderElectionService(GrpcRaftController grpcRaftController) {
        return new LeaderElectionService(grpcRaftController);
    }

    @PreDestroy
    public void clean() {
        GrpcFlowControlledDispatcherListener.shutdown();
    }
}
