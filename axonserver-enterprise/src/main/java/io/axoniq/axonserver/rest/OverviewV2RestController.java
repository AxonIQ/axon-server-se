package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMemberRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;
import io.axoniq.axonserver.topology.Topology;
import org.slf4j.Logger;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marc Gathier
 */
@RestController
@RequestMapping("/v2/overview")
public class OverviewV2RestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final ReplicationGroupMemberRepository raftController;
    private final Map<String, Map<String, ClientDetails>> clientsPerContext = new ConcurrentHashMap<>();
    private final CommandRegistrationCache commandCache;
    private final QueryRegistrationCache queryCache;
    private final String currentNode;

    public OverviewV2RestController(ReplicationGroupMemberRepository raftController,
                                    CommandRegistrationCache commandCache,
                                    QueryRegistrationCache queryCache,
                                    MessagingPlatformConfiguration configuration) {
        this.raftController = raftController;
        this.commandCache = commandCache;
        this.queryCache = queryCache;
        this.currentNode = configuration.getName();
    }

    @GetMapping
    public Printable print(
            @RequestParam(name = "context", required = false, defaultValue = Topology.DEFAULT_CONTEXT) String context,
            Principal principal) {
        auditLog.info("[{}] Request to read configuration for context {}.",
                      AuditLog.username(principal), context);
        return media -> media.with("context", context)
                             .with("nodes", () -> this.nodes(context))
                             .with("applications", () -> this.applications(context));
    }

    @EventListener
    public void on(TopologyEvents.ApplicationConnected applicationConnected) {
        Map<String, ClientDetails> clients = clientsPerContext.computeIfAbsent(applicationConnected.getContext(),
                                                                               c -> new ConcurrentHashMap<>());
        clients.put(applicationConnected.getClient(), new ClientDetails(applicationConnected));
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected applicationDisconnected) {
        clientsPerContext.getOrDefault(applicationDisconnected.getContext(), Collections.emptyMap())
                         .remove(applicationDisconnected.getClient());
    }

    @EventListener
    public void on(EventProcessorEvents.EventProcessorStatusUpdated processorUpdated) {
        ClientDetails clientDetails = clientsPerContext.getOrDefault(processorUpdated.eventProcessorStatus()
                                                                                     .getContext(),
                                                                     Collections.emptyMap())
                                                       .get(processorUpdated.eventProcessorStatus().getClientName());
        if (clientDetails != null) {
            clientDetails.eventProcessorInfo.put(processorUpdated.eventProcessorStatus().getEventProcessorInfo()
                                                                 .getProcessorName(),
                                                 processorUpdated.eventProcessorStatus().getEventProcessorInfo());
        }
    }

    private Iterator<Printable> applications(String context) {
        Map<String, Set<ClientDetails>> clientDetailsPerApplication = new HashMap<>();
        clientsPerContext.getOrDefault(context, Collections.emptyMap())
                         .forEach((client, details) -> clientDetailsPerApplication
                                 .computeIfAbsent(details.component, c -> new HashSet<>()).add(details));

        return clientDetailsPerApplication.entrySet().stream().map(entry -> (Printable) media -> {
            media.with("name", entry.getKey())
                 .with("clients", entry.getValue())
                 .withStrings("commands", () -> commandCache.getAll().entrySet().stream().filter(e -> e.getKey()
                                                                                                       .getClient()
                                                                                                       .getContext()
                                                                                                       .equals(context))
                                                            .filter(e -> e.getKey().getComponentName()
                                                                          .equals(entry.getKey()))
                                                            .flatMap(e -> e.getValue().stream())
                                                            .map(CommandRegistrationCache.RegistrationEntry::getCommand)
                                                            .iterator())
                 .with("queries", () -> queryCache.getAll()
                                                  .entrySet()
                                                  .stream()
                                                  .filter(e -> e.getKey().getContext().equals(context))
                                                  .filter(e -> e.getValue().containsKey(entry.getKey()))
                                                  .map(e -> (Printable) m -> m.with("requestType",
                                                                                    e.getKey().getQueryName())
                                                                              .withStrings("responseTypes",
                                                                                           queryCache
                                                                                                   .getResponseTypes(e.getKey())))
                                                  .iterator());
        }).iterator();
    }

    private Iterator<Printable> nodes(String context) {
        return raftController.findByGroupId(context)
                             .stream()
                             .map(n -> (Printable) media -> media.with("name", n.getNodeName())
                                                                 .with("hostname", n.getHost())
                                                                 .with("port", n.getPort()))
                             .iterator();
    }

    private class ClientDetails implements Printable {

        private final String component;
        private final String proxy;
        private final String client;
        private final Map<String, EventProcessorInfo> eventProcessorInfo = new ConcurrentHashMap<>();

        public ClientDetails(TopologyEvents.ApplicationConnected applicationConnected) {
            this.component = applicationConnected.getComponentName();
            this.client = applicationConnected.getClient();
            this.proxy = applicationConnected.isProxied() ? applicationConnected.getProxy() : currentNode;
        }

        @Override
        public void printOn(Media media) {
            media.with("name", client)
                 .with("connectedTo", proxy)
                 .with("processors", eventProcessorInfo.toString());
        }
    }
}
