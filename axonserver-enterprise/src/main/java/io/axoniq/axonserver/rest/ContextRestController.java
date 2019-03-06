package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
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
    private final RaftLeaderProvider raftLeaderProvider;
    private final ContextController contextController;
    private final FeatureChecker limits;

    public ContextRestController(RaftConfigServiceFactory raftServiceFactory,
                                 RaftLeaderProvider raftLeaderProvider,
                                 ContextController contextController,
                                 FeatureChecker limits) {
        this.raftServiceFactory = raftServiceFactory;
        this.raftLeaderProvider = raftLeaderProvider;
        this.contextController = contextController;
        this.limits = limits;
    }

    @GetMapping(path = "public/context")
    public List<ContextJSON> getContexts() {
        return contextController.getContexts().map(context ->  {
            ContextJSON json = new ContextJSON(context.getName());
            json.setChangePending(context.isChangePending());
            if( context.getPendingSince() != null) {
                json.setPendingSince(context.getPendingSince().getTime());
            }
            json.setLeader(raftLeaderProvider.getLeader(context.getName()));
            json.setNodes(context.getAllNodes().stream().map(n -> n.getClusterNode().getName()).sorted().collect(Collectors.toList()));
            return json;
        }).sorted(Comparator.comparing(ContextJSON::getContext)).collect(Collectors.toList());

    }

    @DeleteMapping( path = "context/{name}")
    public ResponseEntity<RestResponse> deleteContext(@PathVariable("name")  String name) {
        try {
        if( name.startsWith("_")) throw new MessagingPlatformException(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT, String.format("Cannot delete internal context %s", name));
        raftServiceFactory.getRaftConfigService().deleteContext(name);
        return ResponseEntity.accepted()
                             .body(new RestResponse(true,
                                                    "Accepted delete request"));
        } catch( MessagingPlatformException ex) {
            return ResponseEntity.status(ex.getErrorCode().getHttpCode())
                                 .body(new RestResponse(false, ex.getMessage()));
        }
    }

    @PostMapping(path = "context/{context}/{node}")
    public ResponseEntity<RestResponse> updateNodeRoles(@PathVariable("context") String name, @PathVariable("node") String node) {
        try {
            raftServiceFactory.getRaftConfigService().addNodeToContext(name, node);
            return ResponseEntity.accepted()
                          .body(new RestResponse(true,
                                                 "Started to add node to context. This may take some time depending on the number of events already in the context"));
        } catch( MessagingPlatformException ex) {
            return ResponseEntity.status(ex.getErrorCode().getHttpCode())
                                 .body(new RestResponse(false, ex.getMessage()));
        }
    }

    @DeleteMapping(path = "context/{context}/{node}")
    public ResponseEntity<RestResponse> deleteNodeFromContext(@PathVariable("context") String name, @PathVariable("node") String node){
        try {
            raftServiceFactory.getRaftConfigService().deleteNodeFromContext(name, node);
            return ResponseEntity.accepted()
                                 .body(new RestResponse(true,
                                                        "Accepted delete request"));
        } catch( MessagingPlatformException ex) {
            return ResponseEntity.status(ex.getErrorCode().getHttpCode())
                                 .body(new RestResponse(false, ex.getMessage()));
        }
    }

    @PostMapping(path ="context")
    public void addContext(@RequestBody @Valid ContextJSON contextJson) {
        if(!Feature.MULTI_CONTEXT.enabled(limits)) throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED, "License does not allow creating contexts");
        if( contextJson.getNodes() == null || contextJson.getNodes().isEmpty()) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED, "Add at least one node for context");
        }

        if (!contextJson.getContext().matches("[a-zA-Z][a-zA-Z_\\-0-9]*")) {
            throw new MessagingPlatformException(ErrorCode.INVALID_CONTEXT_NAME,
                                                 "Invalid context name");

        }

        raftServiceFactory.getRaftConfigService().addContext(contextJson.getContext(), contextJson.getNodes());
    }

    @PostMapping(path = "context/init")
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
