package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.access.modelversion.ModelVersionController;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.cluster.events.LoadBalancingSynchronizationEvents;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.grpc.ProcessorLoadBalancingProtoConverter;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import org.springframework.context.ApplicationEventPublisher;
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

    private final ModelVersionController modelVersionController;

    private final ProcessorLoadBalancingRepository repository;

    private final ApplicationEventPublisher eventPublisher;

    public ProcessorLoadBalancingController(ModelVersionController modelVersionController,
                                            ProcessorLoadBalancingRepository repository,
                                            ApplicationEventPublisher eventPublisher) {
        this.modelVersionController = modelVersionController;
        this.repository = repository;
        this.eventPublisher = eventPublisher;
    }

    public Optional<ProcessorLoadBalancing> findById(TrackingEventProcessor processor) {
        return repository.findById(processor);
    }

    public void save(ProcessorLoadBalancing processorLoadBalancing){
        repository.save(processorLoadBalancing);
        sync(processorLoadBalancing);
    }

    private void sync(ProcessorLoadBalancing loadBalancing){
        modelVersionController.incrementModelVersion(ProcessorLoadBalancing.class);
        ProcessorLBStrategy processorLBStrategy = new ProcessorLoadBalancingProtoConverter().unmap(loadBalancing);
        eventPublisher.publishEvent(new LoadBalancingSynchronizationEvents.ProcessorLoadBalancingStrategyReceived(processorLBStrategy, false));
    }

    List<ProcessorLoadBalancing> findByStrategy(String strategyName) {
        return repository.findByStrategy(strategyName);
    }

    public List<ProcessorLoadBalancing> findByComponentAndContext(String component, String context) {
        return repository.findByComponentAndContext(component, context);
    }
}
