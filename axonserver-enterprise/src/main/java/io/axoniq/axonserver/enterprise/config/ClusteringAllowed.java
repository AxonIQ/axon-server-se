package io.axoniq.axonserver.enterprise.config;

import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Author: marc
 */
public class ClusteringAllowed implements Condition {

    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        FeatureChecker limits = conditionContext.getBeanFactory().getBean(FeatureChecker.class);
        return Feature.CLUSTERING.enabled(limits);
    }


}
