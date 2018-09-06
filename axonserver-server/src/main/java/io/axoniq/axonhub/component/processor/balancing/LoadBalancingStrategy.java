package io.axoniq.axonhub.component.processor.balancing;

/**
 * Created by Sara Pellegrini on 07/08/2018.
 * sara.pellegrini@gmail.com
 */
public interface LoadBalancingStrategy {

    LoadBalancingOperation balance(TrackingEventProcessor processor);

    interface Factory {

        LoadBalancingStrategy create();

    }

}
