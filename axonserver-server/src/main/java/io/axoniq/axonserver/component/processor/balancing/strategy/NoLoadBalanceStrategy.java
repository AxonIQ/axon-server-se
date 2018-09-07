package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.LoadBalancingOperation;
import io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import org.springframework.stereotype.Component;

/**
 * Created by Sara Pellegrini on 13/08/2018.
 * sara.pellegrini@gmail.com
 */
public class NoLoadBalanceStrategy implements LoadBalancingStrategy {

    @Override
    public LoadBalancingOperation balance(TrackingEventProcessor processor) {
        return () -> {};
    }

    @Component("NoLoadBalance")
    public static final class Factory implements LoadBalancingStrategy.Factory {


        @Override
        public LoadBalancingStrategy create() {
            return new NoLoadBalanceStrategy();
        }
    }
}
