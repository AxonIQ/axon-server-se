package io.axoniq.axonserver.config;

import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.DiskSpaceMetrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.nio.file.FileSystem;

/**
 * Configuration of Axon Server Disk metrics.
 *
 * @author Stefan Dragisic
 * @since 4.5
 */
@Configuration
public class AxonServerDiskMetricsConfig {

    private final Logger logger = LoggerFactory.getLogger(AxonServerDiskMetricsConfig.class);

    private final LocalEventStore localEventStore;

    public AxonServerDiskMetricsConfig(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    /**
     * Registers all necessary views for metric.
     *
     * @param prometheusMeterRegistry micrometer Prometheus registry used for registering gRPC metrics
     */
    @Autowired
    public void configureDiskMetrics(
            PrometheusMeterRegistry prometheusMeterRegistry) {

        localEventStore.registerFileStoreListener(fileStore -> {
            try {
                FileSystem fileSystem = fileStore.getFileSystem();
                DiskSpaceMetrics diskSpaceMetrics = new DiskSpaceMetrics(fileSystem.getPath("/").toFile(), Tags.of("fileSystem", fileSystem.toString()));
                diskSpaceMetrics.bindTo(prometheusMeterRegistry);
            }catch (Exception e) {
                logger.error("Failed to register metrics for file store.");
            }
        });

    }

}
