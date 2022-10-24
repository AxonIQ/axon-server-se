package io.axoniq.axonserver.health;

import io.axoniq.axonserver.commons.health.HealthMonitoredComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthContributorRegistry;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Collects metrics from all health monitored components
 * @author Stefan Dragisic
 */
@Component
public class HealthCheckMonitor {

    protected static final Logger logger = LoggerFactory.getLogger(HealthCheckMonitor.class);

    @Autowired
    public void doRegister(ApplicationContext context, HealthContributorRegistry healthContributorRegistry) {
        Map<String, HealthMonitoredComponent> monitoredComponentMap = context.getBeansOfType(HealthMonitoredComponent.class);

        logger.info("Found {} health monitored components...", monitoredComponentMap.size());

        monitoredComponentMap.values()
                        .stream().filter(it->it.health().status() != HealthMonitoredComponent.Status.IGNORE)
                        .forEach(c -> healthContributorRegistry.registerContributor(c.healthCategory(), new HealthIndicator() {
                            @Override
                            public Health getHealth(boolean includeDetails) {
                                if (includeDetails) {
                                    return Health.status(new Status(c.health().status().name()))
                                            .withDetails(c.health().details()).build();
                                } else {
                                    return health();
                                }
                            }

                            @Override
                            public Health health() {
                                return Health.status(new Status(c.health().status().name())).build();
                            }
                        }));
    }
}
