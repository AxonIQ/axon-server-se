package io.axoniq.axonserver.enterprise.storage.advancedstorage;

import io.axoniq.axonserver.licensing.Limits;
import io.axoniq.axonserver.rest.Feature;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Author: marc
 */
public class MultitierStorageCondition implements Condition {

    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        Limits limits = conditionContext.getBeanFactory().getBean(Limits.class);
        return Feature.MULTI_TIER_STORAGE.enabled(limits);
    }
}
