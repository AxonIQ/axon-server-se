package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.LoadBalancingSynchronizationEvents;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyRepository;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.MessagingClusterService;
import io.axoniq.axonserver.grpc.Converter;
import io.axoniq.axonserver.grpc.LoadBalancingStrategyProtoConverter;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.internal.Action;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.GetLBStrategiesRequest;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.LoadBalancingStrategies;
import io.axoniq.platform.application.ApplicationModelController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import static io.axoniq.axonserver.grpc.internal.ConnectorCommand.RequestCase.REQUEST_LOAD_BALANCING_STRATEGIES;

/**
 * Created by Sara Pellegrini on 16/08/2018.
 * sara.pellegrini@gmail.com
 */
@Controller @Transactional
public class LoadBalancingStrategySynchronizer {

    private final ApplicationModelController applicationController;
    private final LoadBalanceStrategyRepository repository;
    private final Converter<LoadBalanceStrategy, LoadBalancingStrategy> mapping;
    private final Publisher<ConnectorResponse> publisher;

    @Autowired
    public LoadBalancingStrategySynchronizer(ApplicationModelController applicationController,
                                             LoadBalanceStrategyRepository repository,
                                             MessagingClusterService clusterService) {
        this(applicationController, repository,
             message -> clusterService.sendToAll(message, name -> "Error sending load balancing strategy to " + name),
             new LoadBalancingStrategyProtoConverter());

        clusterService.onConnectorCommand(REQUEST_LOAD_BALANCING_STRATEGIES, this::onRequestLBStrategies);
    }

    LoadBalancingStrategySynchronizer(ApplicationModelController applicationController,
                                      LoadBalanceStrategyRepository repository,
                                      Publisher<ConnectorResponse> publisher,
                                      Converter<LoadBalanceStrategy, LoadBalancingStrategy> mapping) {
        this.applicationController = applicationController;
        this.repository = repository;
        this.mapping = mapping;
        this.publisher = publisher;
    }

    @EventListener
    public void on(LoadBalancingSynchronizationEvents.LoadBalancingStrategyReceived event) {
        if( event.isProxied()) {
            Action action = event.strategy().getAction();
            switch (action){
                case DELETE:
                    repository.deleteByName(event.strategy().getName());
                    break;
                case MERGE:
                    repository.save(mapping.map(event.strategy()));
                    break;
            }
        } else {
            publisher.publish(ConnectorResponse.newBuilder().setLoadBalancingStrategy(event.strategy()).build());
        }
    }

    @EventListener
    public void on(LoadBalancingSynchronizationEvents.LoadBalancingStrategiesReceived event) {
        repository.deleteAll();
        repository.flush();
        event.strategies().getStrategyList().forEach(processor -> repository.save(mapping.map(processor)));
        applicationController.updateModelVersion(LoadBalancingStrategy.class, event.strategies().getVersion());
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceConnected event) {
        if (applicationController.getModelVersion(LoadBalancingStrategy.class) < event.getModelVersion(LoadBalancingStrategy.class.getName())) {
            ConnectorCommand command = ConnectorCommand
                    .newBuilder()
                    .setRequestLoadBalancingStrategies(GetLBStrategiesRequest.newBuilder())
                    .build();
            event.getRemoteConnection().publish(command);
        }
    }

    public void onRequestLBStrategies(ConnectorCommand requestStrategy,
                                      Publisher<ConnectorResponse> responsePublisher){
        LoadBalancingStrategies.Builder strategies = LoadBalancingStrategies
                .newBuilder().setVersion(applicationController.getModelVersion(LoadBalancingStrategy.class));
        repository.findAll().forEach(processor -> strategies.addStrategy(mapping.unmap(processor)));
        responsePublisher.publish(ConnectorResponse.newBuilder().setLoadBalancingStrategies(strategies).build());
    }

}
