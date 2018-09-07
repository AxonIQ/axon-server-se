package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.ClusterEvents;
import io.axoniq.axonserver.LoadBalancingSynchronizationEvents;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancingRepository;
import io.axoniq.axonserver.grpc.Converter;
import io.axoniq.axonserver.grpc.ProcessorLoadBalancingProtoConverter;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.internal.MessagingClusterService;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.ConnectorResponse;
import io.axoniq.axonhub.internal.grpc.GetProcessorsLBStrategyRequest;
import io.axoniq.axonhub.internal.grpc.ProcessorsLBStrategy;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.grpc.ProcessorLBStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

import static io.axoniq.axonhub.internal.grpc.ConnectorCommand.RequestCase.REQUEST_PROCESSOR_LOAD_BALANCING_STRATEGIES;

/**
 * Created by Sara Pellegrini on 16/08/2018.
 * sara.pellegrini@gmail.com
 */
@Controller
public class ProcessorLoadBalancingSynchronizer {

    private final ApplicationController applicationController;
    private final ProcessorLoadBalancingRepository repository;
    private final Converter<ProcessorLBStrategy, ProcessorLoadBalancing> mapping;
    private final Publisher<ConnectorResponse> publisher;

    @Autowired
    public ProcessorLoadBalancingSynchronizer(ApplicationController applicationController,
                                              ProcessorLoadBalancingRepository repository,
                                              MessagingClusterService clusterService) {
        this(applicationController, repository,
             message -> clusterService.sendToAll(message, name -> "Error sending processor load balancing strategy to " + name),
             new ProcessorLoadBalancingProtoConverter());

        clusterService.onConnectorCommand(REQUEST_PROCESSOR_LOAD_BALANCING_STRATEGIES, this::onRequestProcessorsStrategies);
    }

    ProcessorLoadBalancingSynchronizer(ApplicationController applicationController,
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
        repository.deleteAll();
        repository.flush();
        event.processorsStrategy().getProcessorList().forEach(processor -> repository.save(mapping.map(processor)));
        applicationController.updateModelVersion(event.processorsStrategy().getVersion());
    }

    @EventListener
    public void on(ClusterEvents.AxonHubInstanceConnected event) {
        if (applicationController.getModelVersion() < event.getModelVersion()) {
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
                .newBuilder().setVersion(applicationController.getModelVersion());
        repository.findAll().forEach(processor -> processors.addProcessor(mapping.unmap(processor)));
        responsePublisher.publish(ConnectorResponse.newBuilder().setProcessorsStrategies(processors).build());
    }

}
