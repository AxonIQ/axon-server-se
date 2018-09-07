package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.cluster.ClusterController;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.ClusterConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SslConfiguration;
import io.axoniq.axonserver.context.ContextController;
import io.axoniq.axonserver.licensing.LicenseConfiguration;
import io.axoniq.axonserver.licensing.Limits;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.subscription.SubscriptionMetrics;
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

    private final ClusterController clusterController;
    private final CommandDispatcher commandDispatcher;
    private final QueryDispatcher queryDispatcher;
    private final EventDispatcher eventDispatcher;
    private final ContextController contextController;
    private final Limits limits;
    private final SslConfiguration sslConfiguration;
    private final AccessControlConfiguration accessControlConfiguration;
    private final ClusterConfiguration clusterConfiguration;
    private final Supplier<SubscriptionMetrics> subscriptionMetricsRegistry;


    public PublicRestController(ClusterController clusterController,
                                CommandDispatcher commandDispatcher,
                                QueryDispatcher queryDispatcher,
                                EventDispatcher eventDispatcher,
                                ContextController contextController,
                                Limits limits,
                                MessagingPlatformConfiguration messagingPlatformConfiguration,
                                Supplier<SubscriptionMetrics> subscriptionMetricsRegistry) {
        this.clusterController = clusterController;
        this.commandDispatcher = commandDispatcher;
        this.queryDispatcher = queryDispatcher;
        this.eventDispatcher = eventDispatcher;
        this.contextController = contextController;
        this.limits = limits;
        this.sslConfiguration = messagingPlatformConfiguration.getSsl();
        this.accessControlConfiguration = messagingPlatformConfiguration.getAccesscontrol();
        this.clusterConfiguration = messagingPlatformConfiguration.getCluster();
        this.subscriptionMetricsRegistry = subscriptionMetricsRegistry;
    }


    @GetMapping
    public List<ClusterNode> getClusterNodes() {
        List<ClusterNode> nodes = clusterController.getRemoteConnections().stream()
                                                   .map(n -> map(n.getClusterNode()))
                                                   .collect(Collectors.toList());

        nodes.add(map(clusterController.getMe()).setConnected(true));
        nodes.sort(Comparator.comparing(ClusterNode::getName));
        return nodes;
    }

    @GetMapping(path = "me")
    public ClusterNode getNodeInfo() {
        ExtendedClusterNode node = mapExtended(clusterController.getMe());
        node.setAuthentication(accessControlConfiguration.isEnabled());
        node.setSsl(sslConfiguration.isEnabled());
        node.setClustered(limits.isClusterAllowed() && clusterConfiguration.isEnabled());
        return node;
    }

    @GetMapping(path = "context")
    public List<ContextJSON> getContexts() {
        return contextController.getContexts().map(ContextJSON::from).collect(Collectors.toList());

    }

    @GetMapping(path="mycontexts")
    public Iterable<String> getMyContextList() {
        return clusterController.getMyContextsNames();
    }



    @GetMapping(path = "license")
    public LicenseInfo licenseInfo() {
        LicenseInfo licenseInfo = new LicenseInfo();
        licenseInfo.setExpiryDate(LicenseConfiguration.getInstance().getExpiryDate());
        licenseInfo.setEdition(LicenseConfiguration.getInstance().getEdition().name());
        licenseInfo.setLicensee(LicenseConfiguration.getInstance().getLicensee());
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

    private ExtendedClusterNode mapExtended(io.axoniq.axonserver.cluster.jpa.ClusterNode me) {
        return new ExtendedClusterNode(me.getName(),
                                       me.getHostName(),
                                       me.getInternalHostName(),
                                       me.getGrpcInternalPort(),
                                       me.getGrpcPort(),
                                       me.getHttpPort());
    }


    private ClusterNode map(io.axoniq.axonserver.cluster.jpa.ClusterNode me) {
        return new ClusterNode(me.getName(), me.getHostName(), me.getInternalHostName(), me.getGrpcInternalPort(),
                               me.getGrpcPort(), me.getHttpPort());
    }
}
