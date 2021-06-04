package io.axoniq.axonserver.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.DiskSpaceMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.autoconfigure.system.DiskSpaceHealthIndicatorProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.system.DiskSpaceHealthIndicator;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Configures File System metrics and health check indicators.
 *
 * Overrides default {@link DiskSpaceHealthIndicator} to set WARN status
 * when the free space drops below the threshold space.
 *
 * @author Stefan Dragisic
 * @since 4.5
 */
@Configuration("diskSpace")
public class FileSystemMonitor extends DiskSpaceHealthIndicator {

    private final Logger logger = LoggerFactory.getLogger(FileSystemMonitor.class);
    private final DiskSpaceHealthIndicatorProperties diskSpaceHealthProperties;

    private final MeterRegistry meterRegistry;

    private final Map<String, Path> fileSystems = new ConcurrentHashMap<>();

    public FileSystemMonitor(DiskSpaceHealthIndicatorProperties diskSpaceHealthProperties,
                             MeterRegistry meterRegistry) {
        super(null,null);
        this.diskSpaceHealthProperties = diskSpaceHealthProperties;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Registers mounted disk based on file path
     * for health monitoring and disk metrics biding
     *
     * */
    public void registerPath(final String name, final Path fileSystemPath) {
        logger.info("Monitoring file store '{}' at path '{}'",name, fileSystemPath);
        fileSystems.put(name, fileSystemPath);
        bindToMetrics(fileSystemPath);
    }

    /**
     * Unregisters file path from health monitoring
     * */
    public void unregisterPath(final String name) {
        logger.info("Stopped monitoring file store '{}'",name);
        fileSystems.remove(name);
    }

    private void bindToMetrics(Path fileSystemPath) {
        try {
            Path mountPath = mountOf(fileSystemPath);

            File f = new File(mountPath.toString());

            DiskSpaceMetrics diskSpaceMetrics = new DiskSpaceMetrics(f);
            diskSpaceMetrics.bindTo(meterRegistry);
        } catch (Exception e) {
            logger.error("Failed to bind disk metrics!",e);
        }
    }

    /**
     * Determines the top level mount point of a directory path
     * */
    private Path mountOf(Path p) throws IOException {
        FileStore fs = Files.getFileStore(p);
        Path temp = p.toAbsolutePath();
        Path mountp = temp;

        while( (temp = temp.getParent()) != null && fs.equals(Files.getFileStore(temp)) ) {
            mountp = temp;
        }
        return mountp;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        builder.up();
        fileSystems.forEach((name, path) -> {
            try {
                FileStore store = Files.getFileStore(path);
                long diskFreeInBytes = store.getUsableSpace();
                long threshold = diskSpaceHealthProperties.getThreshold().toBytes();
                if (store.getUsableSpace() < threshold) {
                    logger.warn(String.format("Free disk space at path '%s' is below threshold. " +
                            "Available: %d bytes (threshold: %s)",path.toString(), diskFreeInBytes, threshold));
                    builder.status(HealthStatus.WARN_STATUS);
                }

                builder.withDetail(name,
                        new Details(
                                mountOf(path).toString(), store.getUsableSpace(), store.getTotalSpace()
                        )
                );

                builder.withDetail("threshold", threshold);
            } catch (
                    Exception e) {
                logger.error("Failed to retrieve file store for {}", path, e);
                builder.down();
                builder.withDetail("path", path.toString());
            }
        });

    }

    public static class Details {
        private final String path;
        private final long free;
        private final long total;

        public Details(String path, long free, long total) {
            this.path = path;
            this.free = free;
            this.total = total;
        }

        public String getPath() {
            return path;
        }

        public long getFree() {
            return free;
        }

        public long getTotal() {
            return total;
        }
    }


}