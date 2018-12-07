package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Comparator;
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

    private final GrpcRaftController grpcRaftController;
    private final ContextController contextController;
    private final FeatureChecker limits;

    public ContextRestController( GrpcRaftController grpcRaftController,
                                  ContextController contextController,
                                  ApplicationEventPublisher applicationEventPublisher,
                                  FeatureChecker limits) {
        this.grpcRaftController = grpcRaftController;
        this.contextController = contextController;
        this.limits = limits;
    }

    @GetMapping(path = "public/context")
    public List<ContextJSON> getContexts() {
        return contextController.getContexts().map(context ->  {
            ContextJSON json = new ContextJSON(context.getName());
            json.setNodes(context.getAllNodes().stream().map(n -> n.getClusterNode().getName()).sorted().collect(Collectors.toList()));
            return json;
        }).sorted(Comparator.comparing(ContextJSON::getContext)).collect(Collectors.toList());

    }

    @DeleteMapping( path = "context/{name}")
    public void deleteContext(@PathVariable("name")  String name) {
        // grpcRaftController.deleteContext(name);
    }

    @PostMapping(path = "context/{context}/{node}")
    public void updateNodeRoles(@PathVariable("context") String name, @PathVariable("node") String node, @RequestParam(name="storage", defaultValue = "true") boolean storage,
                                @RequestParam(name="messaging", defaultValue = "true") boolean messaging
                                 ) {
        grpcRaftController.addNodeToContext(name, node);
    }

    @DeleteMapping(path = "context/{context}/{node}")
    public void deleteNodeFromContext(@PathVariable("context") String name, @PathVariable("node") String node){
        grpcRaftController.deleteNodeFromContext(name, node);
    }

    @PostMapping(path ="context")
    public void addContext(@RequestBody @Valid ContextJSON contextJson) throws Exception {
        if(!Feature.MULTI_CONTEXT.enabled(limits)) throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED, "License does not allow creating contexts");
        grpcRaftController.addContext(contextJson.getContext(), contextJson.getNodes());
    }

    @GetMapping(path = "context/init")
    public void init(@RequestParam(name="context", required = false) List<String> contexts) {
        if( contexts == null) contexts = new ArrayList<>();
        if( contexts.isEmpty()) {
            contexts.add("default");
        }
        grpcRaftController.init(contexts);
    }

}
