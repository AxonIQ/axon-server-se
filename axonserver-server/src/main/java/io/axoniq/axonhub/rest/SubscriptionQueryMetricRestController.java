package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.message.query.subscription.SubscriptionMetrics;
import io.axoniq.axonhub.message.query.subscription.metric.ApplicationSubscriptionMetricRegistry;
import io.axoniq.axonhub.message.query.subscription.metric.QuerySubscriptionMetricRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Sara Pellegrini on 21/06/2018.
 * sara.pellegrini@gmail.com
 */
@RestController
@RequestMapping("/v1/components/{componentName}/subscription-query-metric")
public class SubscriptionQueryMetricRestController {

    private final ApplicationSubscriptionMetricRegistry applicationRegistry;

    private final QuerySubscriptionMetricRegistry queryRegistry;

    public SubscriptionQueryMetricRestController(
            ApplicationSubscriptionMetricRegistry applicationRegistry,
            QuerySubscriptionMetricRegistry queryRegistry) {
        this.applicationRegistry = applicationRegistry;
        this.queryRegistry = queryRegistry;
    }

    @GetMapping
    public SubscriptionMetrics getForComponent(@PathVariable("componentName") String componentName,
                                               @RequestParam("context") String context) {
        return applicationRegistry.get(componentName, context);
    }

    @GetMapping("query/{queryName}")
    public SubscriptionMetrics getForComponentAndQuery(@PathVariable("componentName") String componentName,
                                                       @PathVariable("queryName") String queryName,
                                                       @RequestParam("context") String context) {
        return queryRegistry.get(componentName, queryName, context);
    }
}
