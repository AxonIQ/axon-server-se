package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.Optional;

/**
 * Created by Sara Pellegrini on 16/08/2018.
 * sara.pellegrini@gmail.com
 */
@Controller
@Primary
public class ProcessorLoadBalancingController  {

    private final ProcessorLoadBalancingRepository repository;

    public ProcessorLoadBalancingController(ProcessorLoadBalancingRepository repository) {
        this.repository = repository;
    }

    public Optional<ProcessorLoadBalancing> findById(TrackingEventProcessor processor) {
        return repository.findById(processor);
    }

    public void save(ProcessorLoadBalancing processorLoadBalancing){
        repository.save(processorLoadBalancing);
    }


    List<ProcessorLoadBalancing> findByStrategy(String strategyName) {
        return repository.findByStrategy(strategyName);
    }

    public List<ProcessorLoadBalancing> findByContext(String context) {
        return repository.findByContext(context);
    }
}
