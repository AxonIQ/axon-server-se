package io.axoniq.axonserver.enterprise.storage.advancedstorage;

import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * @author Marc Gathier
 */
public class MultitierStorageCondition implements Condition {

    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        FeatureChecker limits = conditionContext.getBeanFactory().getBean(FeatureChecker.class);
        return Feature.MULTI_TIER_STORAGE.enabled(limits);
    }
}
