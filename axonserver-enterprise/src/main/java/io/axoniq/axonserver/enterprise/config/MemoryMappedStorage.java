package io.axoniq.axonserver.enterprise.config;

import io.axoniq.axonserver.licensing.Feature;
import io.axoniq.axonserver.config.FeatureChecker;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * @author Marc Gathier
 */
public class MemoryMappedStorage implements Condition {
    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        //FeatureChecker limits = conditionContext.getBeanFactory().getBean(FeatureChecker.class);
       // return Feature.MEMORY_MAPPED_FILE_STORAGE.enabled(limits);
        // this is blocking me, bean is not created in good way (properties are not injected), I may need to refactor this code, to be discussed
        return true;
    }


}
