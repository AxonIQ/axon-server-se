package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.ClusterConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SslConfiguration;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.subscription.SubscriptionMetrics;
import io.axoniq.axonserver.rest.json.ExtendedClusterNode;
import io.axoniq.axonserver.rest.json.StatusInfo;
import io.axoniq.axonserver.rest.json.UserInfo;
import io.axoniq.axonserver.topology.AxonServerNode;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.platform.KeepNames;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

/**
 * Author: marc
 */
@RestController("PublicRestController")
@RequestMapping("/v1/public")
public class PublicRestController {

    private final Topology clusterController;
    private final CommandDispatcher commandDispatcher;
    private final QueryDispatcher queryDispatcher;
    private final EventDispatcher eventDispatcher;
    private final FeatureChecker limits;
    private final SslConfiguration sslConfiguration;
    private final AccessControlConfiguration accessControlConfiguration;
    private final ClusterConfiguration clusterConfiguration;
    private final Supplier<SubscriptionMetrics> subscriptionMetricsRegistry;


    public PublicRestController(Topology clusterController,
                                CommandDispatcher commandDispatcher,
                                QueryDispatcher queryDispatcher,
                                EventDispatcher eventDispatcher,
                                FeatureChecker limits,
                                MessagingPlatformConfiguration messagingPlatformConfiguration,
                                Supplier<SubscriptionMetrics> subscriptionMetricsRegistry) {
        this.clusterController = clusterController;
        this.commandDispatcher = commandDispatcher;
        this.queryDispatcher = queryDispatcher;
        this.eventDispatcher = eventDispatcher;
        this.limits = limits;
        this.sslConfiguration = messagingPlatformConfiguration.getSsl();
        this.accessControlConfiguration = messagingPlatformConfiguration.getAccesscontrol();
        this.clusterConfiguration = messagingPlatformConfiguration.getCluster();
        this.subscriptionMetricsRegistry = subscriptionMetricsRegistry;
    }


    @GetMapping
    public List<JsonServerNode> getClusterNodes() {
        List<AxonServerNode> nodes = clusterController.getRemoteConnections();

        nodes.add(clusterController.getMe());
        return nodes.stream()
                    .map(n -> new JsonServerNode(n, clusterController.isActive(n)))
                    .sorted(Comparator.comparing(JsonServerNode::getName)).collect(Collectors.toList());
    }

    @GetMapping(path = "me")
    public ExtendedClusterNode getNodeInfo() {
        ExtendedClusterNode node = mapExtended(clusterController.getMe());
        node.setAuthentication(accessControlConfiguration.isEnabled());
        node.setSsl(sslConfiguration.isEnabled());
        node.setClustered(clusterConfiguration.isEnabled());
        return node;
    }


    @GetMapping(path="mycontexts")
    public Iterable<String> getMyContextList() {
        return clusterController.getMyContextNames();
    }



    @GetMapping(path = "license")
    public LicenseInfo licenseInfo() {
        LicenseInfo licenseInfo = new LicenseInfo();
        licenseInfo.setExpiryDate(limits.getExpiryDate());
        licenseInfo.setEdition(limits.getEdition());
        licenseInfo.setLicensee(limits.getLicensee());
        licenseInfo.setFeatureList(limits.getFeatureList());


        return licenseInfo;
    }

    @GetMapping(path = "status")
    public StatusInfo status() {
        SubscriptionMetrics subscriptionMetrics = this.subscriptionMetricsRegistry.get();
        StatusInfo statusInfo = new StatusInfo();
        statusInfo.setNrOfCommands(commandDispatcher.getNrOfCommands());
        statusInfo.setNrOfQueries(queryDispatcher.getNrOfQueries());
        statusInfo.setNrOfEvents(eventDispatcher.getNrOfEvents());
        statusInfo.setNrOfSnapshots(eventDispatcher.getNrOfSnapshots());
        statusInfo.setEventTrackers(eventDispatcher.eventTrackerStatus());
        statusInfo.setNrOfSubscriptionQueries(subscriptionMetrics.totalCount());
        statusInfo.setNrOfActiveSubscriptionQueries(subscriptionMetrics.activesCount());
        statusInfo.setNrOfSubscriptionQueriesUpdates(subscriptionMetrics.updatesCount());
        return statusInfo;
    }


    @GetMapping(path = "user")
    public UserInfo userInfo(HttpServletRequest request) {
        if (request.getUserPrincipal() instanceof Authentication) {
            Authentication token = (Authentication) request.getUserPrincipal();
            return new UserInfo(token.getName(),
                                token.getAuthorities().stream().map(GrantedAuthority::getAuthority).collect(Collectors.toSet()));
        }

        return null;
    }

    private ExtendedClusterNode mapExtended(AxonServerNode me) {
        return new ExtendedClusterNode(me);
    }


    @KeepNames
    public static class JsonServerNode {

        private final AxonServerNode wrapped;
        private final boolean connected;

        public JsonServerNode(AxonServerNode axonServerNode, boolean connected) {
            this.wrapped = axonServerNode;
            this.connected = connected;
        }


        public String getHostName() {
            return wrapped.getHostName();
        }

        public Integer getGrpcPort() {
            return wrapped.getGrpcPort();
        }

        public String getInternalHostName() {
            return wrapped.getInternalHostName();
        }

        public Integer getGrpcInternalPort() {
            return wrapped.getGrpcInternalPort();
        }

        public Integer getHttpPort() {
            return wrapped.getHttpPort();
        }

        public String getName() {
            return wrapped.getName();
        }

        public boolean isConnected() {
            return connected;
        }
    }
}
