package io.axoniq.axonserver.component.processor.balancing.jpa;

import io.axoniq.platform.application.ApplicationModelController;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Controller;

import java.util.List;

/**
 * Created by Sara Pellegrini on 16/08/2018.
 * sara.pellegrini@gmail.com
 */
@Controller
public class ProcessorLoadBalancingController {

    private final ApplicationModelController applicationController;

    private final ProcessorLoadBalancingRepository repository;

    private final ApplicationEventPublisher eventPublisher;

    public ProcessorLoadBalancingController(ApplicationModelController applicationController,
                                            ProcessorLoadBalancingRepository repository,
                                            ApplicationEventPublisher eventPublisher) {
        this.applicationController = applicationController;
        this.repository = repository;
        this.eventPublisher = eventPublisher;
    }

    public void save(ProcessorLoadBalancing processorLoadBalancing){
        repository.save(processorLoadBalancing);
    }


    List<ProcessorLoadBalancing> findByStrategy(String strategyName) {
        return repository.findByStrategy(strategyName);
    }

    public List<ProcessorLoadBalancing> findByComponentAndContext(String component, String context) {
        return repository.findByComponentAndContext(component, context);
    }
}
