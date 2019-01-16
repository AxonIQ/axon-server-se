package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.cluster.coordinator.AxonHubManager;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;
import javax.validation.Valid;

/**
 * Author: marc
 */
@RestController
@CrossOrigin
@RequestMapping("/v1")
public class ContextRestController {

    private final ContextController contextController;
    private final AxonHubManager axonHubManager;
    private final EventStoreLocator eventStoreManager;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final FeatureChecker limits;

    public ContextRestController( ContextController contextController, AxonHubManager axonHubManager, EventStoreLocator eventStoreManager,
                                  ApplicationEventPublisher applicationEventPublisher,
                                  FeatureChecker limits) {
        this.contextController = contextController;
        this.axonHubManager = axonHubManager;
        this.eventStoreManager = eventStoreManager;
        this.applicationEventPublisher = applicationEventPublisher;
        this.limits = limits;
    }

    @GetMapping(path = "public/context")
    public List<ContextJSON> getContexts() {
        return contextController.getContexts().map(this::createContextJSON).collect(Collectors.toList());

    }
    @GetMapping(path = "context/init")
    public ResponseEntity<RestResponse> initCluster() {
        return ResponseEntity.ok(new RestResponse(true, null));
    }

    private ContextJSON createContextJSON(Context context) {
        ContextJSON contextJSON = ContextJSON.from(context);
        contextJSON.setMaster(eventStoreManager.getMaster(context.getName()));
        contextJSON.setCoordinator(axonHubManager.coordinatorFor(context.getName()));
        return contextJSON;
    }

    @DeleteMapping( path = "context/{name}")
    public void deleteContext(@PathVariable("name")  String name) {
        if( Topology.DEFAULT_CONTEXT.equals(name)) throw new MessagingPlatformException(ErrorCode.CANNOT_DELETE_DEFAULT, "Cannot delete default context");
        contextController.canDeleteContext(name);
        applicationEventPublisher.publishEvent(contextController.deleteContext(name, false));
    }

    @PostMapping(path = "context/{context}/{node}")
    public void updateNodeRoles(@PathVariable("context") String name, @PathVariable("node") String node, @RequestParam(name="storage", defaultValue = "true") boolean storage,
                                @RequestParam(name="messaging", defaultValue = "true") boolean messaging
                                 ) {
        contextController.canUpdateContext(name, node);
        applicationEventPublisher.publishEvent(contextController.updateNodeRoles(name,
                                                                                 node,
                                                                                 storage,
                                                                                 messaging,
                                                                                 false));
    }

    @DeleteMapping(path = "context/{context}/{node}")
    public void deleteNodeFromContext(@PathVariable("context") String name, @PathVariable("node") String node){
        contextController.canUpdateContext(name, node);
        applicationEventPublisher.publishEvent(contextController.deleteNodeFromContext(name, node, false));
    }

    @PatchMapping(path = "context/{context}/move")
    public void releaseContextMaster(@PathVariable("context") String name) {
        applicationEventPublisher.publishEvent(new ClusterEvents.MasterStepDown(name, false));
    }

    @PatchMapping(path = "context/{context}/coordinator/move")
    public void releaseContextCoordinator(@PathVariable("context") String name) {
        applicationEventPublisher.publishEvent(new ClusterEvents.CoordinatorStepDown(name, false));
    }

    @PostMapping(path ="context")
    public void addContext(@RequestBody @Valid ContextJSON contextJson) {
        if (!Feature.MULTI_CONTEXT.enabled(limits)) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED,
                                                 "License does not allow creating contexts");
        }
        if( contextJson.getNodes() == null || contextJson.getNodes().isEmpty()) {
            throw new MessagingPlatformException(ErrorCode.SELECT_NODES_FOR_CONTEXT,
                                                 "Add at least one node to context");
        }
        if (!contextJson.getContext().matches("[a-zA-Z][a-zA-Z_-]*")) {
            throw new MessagingPlatformException(ErrorCode.INVALID_CONTEXT_NAME,
                                                 "Invalid context name");

        }
        contextController.canAddContext(contextJson.getNodes());
        applicationEventPublisher.publishEvent(contextController.addContext(contextJson.getContext(),
                                                                            contextJson.getNodes(),
                                                                            false));
    }

}
