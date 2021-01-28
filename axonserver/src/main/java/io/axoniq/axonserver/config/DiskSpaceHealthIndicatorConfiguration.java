package io.axoniq.axonserver.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.actuate.autoconfigure.system.DiskSpaceHealthIndicatorProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.unit.DataSize;

import java.io.File;

/**
 * Overrides default {@link DiskSpaceHealthIndicator} to set WARN status
 * when the free space drops below the threshold space.
 *
 * @author Stefan Dragisic
 * @since 4.5
 */
@Configuration
public class DiskSpaceHealthIndicatorConfiguration {

    @Bean
    public org.springframework.boot.actuate.system.DiskSpaceHealthIndicator diskSpaceHealthIndicator(DiskSpaceHealthIndicatorProperties properties) {
        return new DiskSpaceHealthIndicator(properties.getPath(), properties.getThreshold());
    }

    private static class DiskSpaceHealthIndicator extends org.springframework.boot.actuate.system.DiskSpaceHealthIndicator {

        private static final Log logger = LogFactory.getLog(DiskSpaceHealthIndicatorConfiguration.class);
        private final File path;
        private final DataSize threshold;

        public DiskSpaceHealthIndicator(File path, DataSize threshold) {
            super(path,threshold);
            this.path = path;
            this.threshold = threshold;
        }

        @Override
        protected void doHealthCheck(Health.Builder builder) {
            long diskFreeInBytes = this.path.getUsableSpace();
            if (diskFreeInBytes >= this.threshold.toBytes()) {
                builder.up();
            } else {
                logger.warn(String.format("Free disk space below threshold. Available: %d bytes (threshold: %s)", diskFreeInBytes, this.threshold));
                builder.status(new Status("WARN"));
            }

            builder.withDetail("total", this.path.getTotalSpace()).withDetail("free", diskFreeInBytes).withDetail("threshold", this.threshold.toBytes());
        }
    }
}