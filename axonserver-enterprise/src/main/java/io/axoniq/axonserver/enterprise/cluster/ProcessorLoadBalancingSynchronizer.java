package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.access.modelversion.ModelVersionController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.events.LoadBalancingSynchronizationEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.MessagingClusterService;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ProcessorLoadBalancingRepository;
import io.axoniq.axonserver.grpc.Converter;
import io.axoniq.axonserver.grpc.ProcessorLoadBalancingProtoConverter;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.GetProcessorsLBStrategyRequest;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.ProcessorsLBStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

import static io.axoniq.axonserver.grpc.internal.ConnectorCommand.RequestCase.REQUEST_PROCESSOR_LOAD_BALANCING_STRATEGIES;

/**
 * Created by Sara Pellegrini on 16/08/2018.
 * sara.pellegrini@gmail.com
 */
@Controller
public class ProcessorLoadBalancingSynchronizer {

    private final ModelVersionController applicationController;
    private final ProcessorLoadBalancingRepository repository;
    private final Converter<ProcessorLBStrategy, ProcessorLoadBalancing> mapping;
    private final Publisher<ConnectorResponse> publisher;

    @Autowired
    public ProcessorLoadBalancingSynchronizer(ModelVersionController applicationController,
                                              ProcessorLoadBalancingRepository repository,
                                              MessagingClusterService clusterService) {
        this(applicationController, repository,
             message -> clusterService.sendToAll(message, name -> "Error sending processor load balancing strategy to " + name),
             new ProcessorLoadBalancingProtoConverter());

        clusterService.onConnectorCommand(REQUEST_PROCESSOR_LOAD_BALANCING_STRATEGIES, this::onRequestProcessorsStrategies);
    }

    ProcessorLoadBalancingSynchronizer(ModelVersionController applicationController,
                                       ProcessorLoadBalancingRepository repository,
                                       Publisher<ConnectorResponse> publisher,
                                       Converter<ProcessorLBStrategy, ProcessorLoadBalancing> mapping) {
        this.applicationController = applicationController;
        this.repository = repository;
        this.mapping = mapping;
        this.publisher = publisher;
    }

    @EventListener
    public void on(LoadBalancingSynchronizationEvents.ProcessorLoadBalancingStrategyReceived event) {
        if (event.isProxied()){
            repository.save(mapping.map(event.processorLBStrategy()));
        } else {
            publisher.publish(ConnectorResponse.newBuilder().setProcessorStrategy(event.processorLBStrategy()).build());
        }
    }

    @EventListener
    public void on(LoadBalancingSynchronizationEvents.ProcessorsLoadBalanceStrategyReceived event) {
        synchronized (repository) {
            repository.deleteAll();
            repository.flush();
            event.processorsStrategy().getProcessorList().forEach(processor -> repository.save(mapping.map(processor)));
        }
        applicationController.updateModelVersion(ProcessorLoadBalancing.class, event.processorsStrategy().getVersion());
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceConnected event) {
        // TODO
        if (applicationController.getModelVersion(ProcessorLoadBalancing.class) < event.getModelVersion(ProcessorLoadBalancing.class.getName())) {
            ConnectorCommand command = ConnectorCommand
                    .newBuilder()
                    .setRequestProcessorLoadBalancingStrategies(GetProcessorsLBStrategyRequest.newBuilder())
                    .build();
            event.getRemoteConnection().publish(command);
        }
    }

    public void onRequestProcessorsStrategies(ConnectorCommand requestStrategy,
                                              Publisher<ConnectorResponse> responsePublisher){
        ProcessorsLBStrategy.Builder processors = ProcessorsLBStrategy
                .newBuilder().setVersion(applicationController.getModelVersion(ProcessorLoadBalancing.class));
        repository.findAll().forEach(processor -> processors.addProcessor(mapping.unmap(processor)));
        responsePublisher.publish(ConnectorResponse.newBuilder().setProcessorsStrategies(processors).build());
    }

}
