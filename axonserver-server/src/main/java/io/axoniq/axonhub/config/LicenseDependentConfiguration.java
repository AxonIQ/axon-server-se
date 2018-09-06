package io.axoniq.axonhub.config;

import com.codahale.metrics.MetricRegistry;
import io.axoniq.axonhub.message.query.MetricsBasedQueryHandlerSelector;
import io.axoniq.axonhub.message.query.QueryHandlerSelector;
import io.axoniq.axonhub.message.query.QueryMetricsRegistry;
import io.axoniq.axonhub.message.query.RoundRobinQueryHandlerSelector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Author: marc
 */
@Configuration
public class LicenseDependentConfiguration {
    @Bean
    @Primary
    public QueryHandlerSelector queryHandlerSelector(QueryMetricsRegistry queryMetricsRegistry) {
        if(io.axoniq.axonhub.licensing.LicenseConfiguration.isEnterprise()) {
            return new MetricsBasedQueryHandlerSelector(queryMetricsRegistry);
        }
        return new RoundRobinQueryHandlerSelector();
    }

    @Bean
    public MetricRegistry metricRegistry() {
        return new MetricRegistry();
    }

}
