package io.axoniq.axonserver.config;

import io.axoniq.axonserver.message.query.MetricsBasedQueryHandlerSelector;
import io.axoniq.axonserver.message.query.QueryHandlerSelector;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.message.query.RoundRobinQueryHandlerSelector;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

/**
 * Author: marc
 */
@Configuration
public class LicenseDependentConfiguration {
    @Bean
    @Primary
    public QueryHandlerSelector queryHandlerSelector(QueryMetricsRegistry queryMetricsRegistry) {
        if(io.axoniq.axonserver.licensing.LicenseConfiguration.isEnterprise()) {
            return new MetricsBasedQueryHandlerSelector(queryMetricsRegistry);
        }
        return new RoundRobinQueryHandlerSelector();
    }

    @Bean
    MeterRegistryCustomizer<MeterRegistry> metricsCommonTags(MessagingPlatformConfiguration messagingPlatformConfiguration) {
        MeterFilter defaultFilter = new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id, @NotNull DistributionStatisticConfig config) {
                if (id.getName().startsWith("axon")) {
                    return DistributionStatisticConfig.builder()
                                                      .percentiles(0.5, 0.95, 0.99)
                                                      .minimumExpectedValue(TimeUnit.MILLISECONDS.toMillis(1))
                                                      .maximumExpectedValue(TimeUnit.SECONDS.toMillis(10))
                                                      .expiry(Duration.ofMinutes(messagingPlatformConfiguration.getMetricsInterval()))
                                                      .build()
                                                      .merge(config);
                }
                return config;
            }
        };
        return registry -> registry.config().meterFilter(defaultFilter).
                commonTags("axonserver", messagingPlatformConfiguration.getName());
    }
}
