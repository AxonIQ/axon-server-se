package io.axoniq.axonserver.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.enterprise.ClusterTemplateController;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.DistributeLicenseService;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.config.ClusterTemplate;
import io.axoniq.axonserver.enterprise.context.ContextNameValidation;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.topology.ClusterTopology;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.UpdateLicense;
import io.axoniq.axonserver.licensing.Feature;
import io.axoniq.axonserver.licensing.LicenseException;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.rest.json.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Marc Gathier
 */
@RestController("ClusterRestController")
@RequestMapping("/v1/cluster")
public class ClusterRestController {

    private static final Logger auditLog = AuditLog.getLogger();
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String CONTEXT_NONE = "_none";

    private final ClusterController clusterController;
    private final RaftConfigServiceFactory raftServiceFactory;
    private final FeatureChecker limits;
    private final Predicate<String> contextNameValidation = new ContextNameValidation();
    private final DistributeLicenseService distributeLicenseService;
    private final ApplicationEventPublisher eventPublisher;
    private final ClusterTopology clusterTopology;
    private final ClusterTemplateController clusterTemplateController;

    public ClusterRestController(ClusterController clusterController,
                                 RaftConfigServiceFactory raftServiceFactory,
                                 FeatureChecker limits,
                                 DistributeLicenseService distributeLicenseService,
                                 ApplicationEventPublisher eventPublisher,
                                 ClusterTopology clusterTopology,
                                 ClusterTemplateController clusterTemplateController) {
        this.clusterController = clusterController;
        this.raftServiceFactory = raftServiceFactory;
        this.limits = limits;
        this.distributeLicenseService = distributeLicenseService;
        this.eventPublisher = eventPublisher;
        this.clusterTopology = clusterTopology;
        this.clusterTemplateController = clusterTemplateController;
    }


    @PostMapping
    public ResponseEntity<RestResponse> add(@Valid @RequestBody ClusterJoinRequest jsonClusterNode,
                                            Principal principal) {
        auditLog.info("[{}] Request to join cluster at {}:{}.",
                      AuditLog.username(principal),
                      jsonClusterNode.getInternalHostName(),
                      jsonClusterNode.internalGrpcPort);
        NodeInfo.Builder nodeInfoBuilder = NodeInfo.newBuilder(clusterController.getMe().toNodeInfo());
        String context = jsonClusterNode.getContext();
        // Check for both context and noContext
        if (context != null && !context.isEmpty()) {
            if ((jsonClusterNode.getNoContexts() != null) && jsonClusterNode.getNoContexts()) {
                throw new MessagingPlatformException(ErrorCode.INVALID_CONTEXT_NAME,
                                                     "Cannot combine joining context with noContexts.");
            } else if (!isAdmin(context) && !contextNameValidation.test(context)) {
                throw new MessagingPlatformException(ErrorCode.INVALID_CONTEXT_NAME,
                                                     "Invalid context name: " + context);
            }
            logger.debug("add(): Registering myself and adding me to context \"{}\".", context);
            nodeInfoBuilder.addContexts(ContextRole.newBuilder().setName(context).build());
        } else if ((jsonClusterNode.getNoContexts() != null) && jsonClusterNode.getNoContexts()) {
            logger.debug("add(): Registering myself and adding me to no contexts.");
            nodeInfoBuilder.addContexts(ContextRole.newBuilder().setName(CONTEXT_NONE).build());
        } else {
            logger.debug("add(): Registering myself and adding me to all contexts.");
        }

        // Check if node is already registered in a cluster
        if (clusterController.nodes().count() > 1) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                                 .body(new RestResponse(false,
                                                        "Node already registered in a cluster"));
        }

        try {
            UpdateLicense updateLicense = raftServiceFactory.getRaftConfigServiceStub(jsonClusterNode.internalHostName,
                    jsonClusterNode.internalGrpcPort)
                    .joinCluster(nodeInfoBuilder.build());

            eventPublisher.publishEvent(new ClusterEvents.LicenseUpdated(updateLicense.getLicense().toByteArray()));

            return ResponseEntity.accepted()
                                 .body(new RestResponse(true,
                                                        "Accepted join request, may take a while to process"));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }


    @DeleteMapping(path = "{name}")
    public void deleteNode(@PathVariable("name") String name, Principal principal) {
        auditLog.info("[{}] Request to delete node {}.",
                      AuditLog.username(principal),
                      name);
        if (!Feature.CLUSTERING.enabled(limits)) {
            throw new MessagingPlatformException(ErrorCode.CLUSTER_NOT_ALLOWED,
                                                 "License does not allow clustering of Axon servers");
        }
        raftServiceFactory.getRaftConfigService().deleteNode(name);
    }


    @GetMapping
    public List<JsonClusterNode> list(Principal principal) {
        auditLog.info("[{}] Request to list nodes.",
                      AuditLog.username(principal));
        return clusterController
                .nodes()
                .map(e -> JsonClusterNode.from(e, clusterController.isActive(e.getName())))
                .collect(Collectors.toList());
    }

    @GetMapping("/download-template")
    public @ResponseBody
    void downloadClusterTemplate(HttpServletResponse resp, Principal principal) throws IOException {
        auditLog.info("[{}] Request cluster template download.",
                AuditLog.username(principal));

        if (clusterTopology.isAdminNode()) {
            String downloadFileName = "cluster-template.yml";
            YAMLFactory yamlFactory = new YAMLFactory()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                    .enable(YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS)
                    .disable(YAMLGenerator.Feature.INDENT_ARRAYS)
                    .disable(YAMLGenerator.Feature.SPLIT_LINES);

            ObjectMapper mapper = new ObjectMapper(yamlFactory);

            ClusterTemplate clusterTemplate = clusterTemplateController.buildTemplate();

            Map<String, Map<String, ClusterTemplate>> clusterTemplateWithRoot =
                    Collections.singletonMap("axonserver",
                            Collections.singletonMap("cluster-template", clusterTemplate)
                    );

            String downloadStringContent = mapper
                    .writer()
                    .withRootName("axoniq")
                    .writeValueAsString(clusterTemplateWithRoot);

            OutputStream out = resp.getOutputStream();
            resp.setContentType("text/plain; charset=utf-8");
            resp.addHeader("Content-Disposition", "attachment; filename=\"" + downloadFileName + "\"");
            out.write(downloadStringContent.getBytes(UTF_8));
            out.flush();
            out.close();
        } else {
            throw new RuntimeException("You can use this functionality only from admin node");
        }
    }

    @PostMapping("/upload-license")
    public void distributeLicense(@RequestParam("licenseFile") MultipartFile licenseFile, Principal principal)
            throws IOException {
        auditLog.info("[{}] Request license update.",
                      AuditLog.username(principal));
        String axoniq_license = System.getenv("AXONIQ_LICENSE");
        if (axoniq_license != null) {
            throw new MessagingPlatformException(ErrorCode.INVALID_PROPERTY_VALUE,
                                                 "License path hardcoded to AXONIQ_LICENSE=" + axoniq_license
                                                         + ". Remove this environment key to dynamically manage license.");
        }

        logger.info("New license uploaded, performing license update...");
        try {
            distributeLicenseService.distributeLicense(licenseFile.getBytes());
        } catch (LicenseException licenseException) {
            throw new MessagingPlatformException(ErrorCode.INVALID_PROPERTY_VALUE, licenseException.getMessage());
        }
    }

    @GetMapping(path = "{name}")
    public JsonClusterNode getOne(@PathVariable("name") String name, Principal principal) {
        auditLog.info("[{}] Request node details for {}.",
                      AuditLog.username(principal), name);
        ClusterNode node = clusterController.getNode(name);
        if (node == null) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_NODE, "Node " + name + " not found");
        }

        return JsonClusterNode.from(node, clusterController.isActive(name));
    }

    @KeepNames
    public static class JsonClusterNode {
        private String name;
        private String internalHostName;
        private Integer internalGrpcPort;
        private String hostName;
        private Integer grpcPort;
        private Integer httpPort;
        private boolean connected;
        private Map<String,String> tags;

        public boolean isConnected() {
            return connected;
        }

        public void setConnected(boolean connected) {
            this.connected = connected;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getInternalHostName() {
            return internalHostName;
        }

        public void setInternalHostName(String internalHostName) {
            this.internalHostName = internalHostName;
        }

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
        }

        public Integer getHttpPort() {
            return httpPort;
        }

        public void setHttpPort(Integer httpPort) {
            this.httpPort = httpPort;
        }

        public Integer getInternalGrpcPort() {
            return internalGrpcPort;
        }

        public void setInternalGrpcPort(Integer internalGrpcPort) {
            this.internalGrpcPort = internalGrpcPort;
        }

        public Integer getGrpcPort() {
            return grpcPort;
        }

        public void setGrpcPort(Integer grpcPort) {
            this.grpcPort = grpcPort;
        }

        public Map<String,String> getTags(){
            return tags;
        }

        public void setTags(Map<String,String> tags){
            this.tags = tags;
        }

        public static JsonClusterNode from(ClusterNode jpaClusterNode, boolean connected) {
            JsonClusterNode clusterNode = new JsonClusterNode();
            clusterNode.name = jpaClusterNode.getName();
            clusterNode.internalHostName = jpaClusterNode.getInternalHostName();
            clusterNode.internalGrpcPort = jpaClusterNode.getGrpcInternalPort();
            clusterNode.hostName = jpaClusterNode.getHostName();
            clusterNode.grpcPort = jpaClusterNode.getGrpcPort();
            clusterNode.httpPort = jpaClusterNode.getHttpPort();
            clusterNode.connected = connected;
            clusterNode.tags = jpaClusterNode.getTags();
            return clusterNode;
        }
    }

    @KeepNames
    public static class ClusterJoinRequest {
        @NotNull(message = "missing required field: internalHostName")
        private String internalHostName;
        @NotNull(message = "missing required field: internalGrpcPort")
        private Integer internalGrpcPort;

        private String context;
        private Boolean noContexts;

        public String getInternalHostName() {
            return internalHostName;
        }

        public void setInternalHostName(String internalHostName) {
            this.internalHostName = internalHostName;
        }

        public Integer getInternalGrpcPort() {
            return internalGrpcPort;
        }

        public void setInternalGrpcPort(@NotNull Integer internalGrpcPort) {
            this.internalGrpcPort = internalGrpcPort;
        }

        /**
         * Return the context this node should join. If {@code null} and {@link #noContexts} equals {@code true},
         * then the node should join no context. If both are {@code null}, the node should join all contexts.
         *
         * @return the context to join.
         */
        public String getContext() {
            return context;
        }

        /**
         * Set the context to join.
         *
         * @param context the name of the context, or {@code null} for all/none. See {@link #getContext()} and {@link #getNoContexts()}.
         */
        public void setContext(String context) {
            this.context = context;
        }

        /**
         * Return the (optional) field to indicate if this node should be joined to no contexts. A value of {@code null}
         * indicates that the old behaviour is expected, which means that the node will be added to a single or all
         * contexts known to the "_admin" leader. If {@code true}, {@link #context} should not be used.
         *
         * @return {@code true} if the node should be joined to no contexts.
         */
        public Boolean getNoContexts() {
            return noContexts;
        }

        /**
         * Set the (optional) {@link #noContexts} field. Typically, only {@code true} or {@code null} make sense.
         *
         * @param noContexts the value for this field.
         */
        public void setNoContexts(Boolean noContexts) {
            this.noContexts = noContexts;
        }
    }
}
