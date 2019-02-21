package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.cluster.RaftGroupServiceFactory;
import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.rest.json.RestResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
 * @author Marc Gathier
 */
@RestController
@CrossOrigin
@RequestMapping("/v1")
public class ContextRestController {

    private final RaftConfigServiceFactory raftServiceFactory;
    private final RaftGroupServiceFactory raftGroupServiceFactory;
    private final RaftLeaderProvider raftLeaderProvider;
    private final ContextController contextController;
    private final FeatureChecker limits;

    public ContextRestController(RaftConfigServiceFactory raftServiceFactory,
                                 RaftGroupServiceFactory raftGroupServiceFactory,
                                 RaftLeaderProvider raftLeaderProvider,
                                 ContextController contextController,
                                 FeatureChecker limits) {
        this.raftServiceFactory = raftServiceFactory;
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.raftLeaderProvider = raftLeaderProvider;
        this.contextController = contextController;
        this.limits = limits;
    }

    @GetMapping(path = "public/context")
    public List<ContextJSON> getContexts() {
        return contextController.getContexts().map(context ->  {
            ContextJSON json = new ContextJSON(context.getName());
            json.setLeader(raftLeaderProvider.getLeader(context.getName()));
            json.setNodes(context.getAllNodes().stream().map(n -> n.getClusterNode().getName()).sorted().collect(Collectors.toList()));
            return json;
        }).sorted(Comparator.comparing(ContextJSON::getContext)).collect(Collectors.toList());

    }

    @DeleteMapping( path = "context/{name}")
    public void deleteContext(@PathVariable("name")  String name) {
        if( name.startsWith("_")) throw new MessagingPlatformException(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT, String.format("Cannot delete internal context %s", name));
        raftServiceFactory.getRaftConfigService().deleteContext(name);
    }

    @PostMapping(path = "context/{context}/{node}")
    public void updateNodeRoles(@PathVariable("context") String name, @PathVariable("node") String node) {
        raftServiceFactory.getRaftConfigService().addNodeToContext(name, node);
    }

    @DeleteMapping(path = "context/{context}/{node}")
    public void deleteNodeFromContext(@PathVariable("context") String name, @PathVariable("node") String node){
        raftServiceFactory.getRaftConfigService().deleteNodeFromContext(name, node);
    }

    @PostMapping(path ="context")
    public void addContext(@RequestBody @Valid ContextJSON contextJson) {
        if(!Feature.MULTI_CONTEXT.enabled(limits)) throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED, "License does not allow creating contexts");
        if( contextJson.getNodes() == null || contextJson.getNodes().isEmpty()) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED, "Add at least one node for context");
        }
        raftServiceFactory.getRaftConfigService().addContext(contextJson.getContext(), contextJson.getNodes());
    }

    @GetMapping(path = "context/init")
    public ResponseEntity<RestResponse> init(@RequestParam(name="context", required = false) List<String> contexts) {
        if( contexts == null) contexts = new ArrayList<>();
        if( contexts.isEmpty()) {
            contexts.add("default");
        }
        try {
            raftServiceFactory.getLocalRaftConfigService().init(contexts);
            return ResponseEntity.ok(new RestResponse(true, null));
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new RestResponse(false, ex.getMessage()));
        }
    }

}
