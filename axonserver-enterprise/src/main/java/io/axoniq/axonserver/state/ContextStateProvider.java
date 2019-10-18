package io.axoniq.axonserver.state;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.internal.saas.Client;
import io.axoniq.axonserver.grpc.internal.saas.ClientApplication;
import io.axoniq.axonserver.grpc.internal.saas.ContextOverview;
import io.axoniq.axonserver.grpc.internal.saas.QueryInfo;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Component
public class ContextStateProvider {

    private final ContextController contextController;
    private final Map<String, Map<String, ComponentDetails>> componentsPerContext
            = new ConcurrentHashMap<>();
    private final String currentNode;


    public ContextStateProvider(ClusterController clusterController, ContextController contextController) {
        this.contextController = contextController;
        this.currentNode = clusterController.getName();
    }

    public ContextOverview getCurrentState(String context) {
        ContextOverview.Builder builder = ContextOverview.newBuilder()
                                                         .setContext(context);
        if (contextController.getContext(context) == null) {
            throw GrpcExceptionBuilder.build(ErrorCode.CONTEXT_NOT_FOUND,
                                             "Context not found");
        }

        Map<String, ComponentDetails> applications = componentsPerContext.getOrDefault(context, Collections.emptyMap());
        applications.forEach((appName, details) -> {
            ClientApplication.Builder clientApp = ClientApplication.newBuilder().setName(appName);

            if (!details.clients.isEmpty()) {
                String firstClient = details.clients.keySet().iterator().next();
                ClientDetails clientDetails = details.clients.get(firstClient);
                clientApp.addAllCommands(clientDetails.commands);

                clientApp.addAllQueries(clientDetails
                                                .queries
                                                .entrySet()
                                                .stream()
                                                .map(q -> QueryInfo.newBuilder()
                                                                   .setRequest(q.getKey())
                                                                   .addAllResponseTypes(q.getValue())
                                                                   .build())
                                                .collect(Collectors.toList()));

                details.clients.forEach((clientId, clientDetail) -> {
                    Client.Builder clientBuilder = Client.newBuilder().setName(clientId).setNode(clientDetails.proxy);
                    clientBuilder.addAllProcessors(clientDetail.eventProcessorInfo.values());
                    clientApp.addClients(clientBuilder);
                });
            }
            builder.addApplications(clientApp.build());
        });

        contextController.getContext(context).getAllNodes().forEach(n -> builder
                .addNodes(n.getClusterNode().getName()));

        return builder.build();
    }

    @EventListener
    public void on(TopologyEvents.ApplicationConnected applicationConnected) {
        Map<String, ComponentDetails> clients = componentsPerContext.computeIfAbsent(applicationConnected.getContext(),
                                                                                     c -> new ConcurrentHashMap<>());
        clients.computeIfAbsent(applicationConnected.getComponentName(),
                                c -> new ComponentDetails()).applicationConnected(applicationConnected);
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected applicationDisconnected) {
        Map<String, ComponentDetails> contextDetailsMap = componentsPerContext.getOrDefault(applicationDisconnected
                                                                                                    .getContext(),
                                                                                            Collections.emptyMap());
        Set<String> componentsToDelete = new HashSet<>();
        contextDetailsMap.forEach((component, contextDetails) -> {
            contextDetails.applicationDisconnected(applicationDisconnected);
            if (contextDetails.clients.isEmpty()) {
                componentsToDelete.add(component);
            }
        });

        componentsToDelete.forEach(contextDetailsMap::remove);
    }

    @EventListener
    public void on(EventProcessorEvents.EventProcessorStatusUpdated processorUpdated) {
        componentsPerContext.getOrDefault(processorUpdated.eventProcessorStatus().getContext(),
                                          Collections.emptyMap()).forEach((c, d) -> d
                .updateProcessorStatus(processorUpdated.eventProcessorStatus()));
    }

    @EventListener
    public void on(SubscriptionEvents.SubscribeCommand subscribeCommand) {
        ComponentDetails componentDetails = getComponent(subscribeCommand.clientIdentification().getContext(),
                                                         subscribeCommand.getRequest().getComponentName());
        if (componentDetails != null) {
            componentDetails.subscribeCommand(subscribeCommand.getRequest().getClientId(),
                                              subscribeCommand.getRequest().getCommand());
        }
    }

    @EventListener
    public void on(SubscriptionEvents.UnsubscribeCommand unsubscribeCommand) {
        ComponentDetails componentDetails = getComponent(unsubscribeCommand.clientIdentification().getContext(),
                                                         unsubscribeCommand.getRequest().getComponentName());
        if (componentDetails != null) {
            componentDetails.unsubscribeCommand(unsubscribeCommand.getRequest().getClientId(),
                                                unsubscribeCommand.getRequest().getCommand());
        }
    }

    private ComponentDetails getComponent(String context, String componentName) {
        return componentsPerContext.getOrDefault(context, Collections.emptyMap())
                                   .get(componentName);
    }


    @EventListener
    public void on(SubscriptionEvents.SubscribeQuery subscribeQuery) {
        ComponentDetails componentDetails = getComponent(subscribeQuery.clientIdentification().getContext(),
                                                         subscribeQuery.getSubscription().getComponentName());
        if (componentDetails != null) {
            componentDetails.subscribeQuery(subscribeQuery.clientIdentification().getClient(),
                                            subscribeQuery.getSubscription().getQuery(),
                                            subscribeQuery.getSubscription().getResultName());
        }
    }

    @EventListener
    public void on(SubscriptionEvents.UnsubscribeQuery unsubscribeQuery) {
        ComponentDetails componentDetails = getComponent(unsubscribeQuery.clientIdentification().getContext(),
                                                         unsubscribeQuery.getUnsubscribe().getComponentName());
        if (componentDetails != null) {
            componentDetails.unsubscribeQuery(unsubscribeQuery.clientIdentification().getClient(),
                                              unsubscribeQuery.getUnsubscribe().getQuery(),
                                              unsubscribeQuery.getUnsubscribe().getResultName());
        }
    }

    private class ClientDetails {

        private final String proxy;
        private final Set<String> commands = new CopyOnWriteArraySet<>();
        private final Map<String, Set<String>> queries = new ConcurrentHashMap<>();
        private final Map<String, EventProcessorInfo> eventProcessorInfo = new ConcurrentHashMap<>();

        public ClientDetails(TopologyEvents.ApplicationConnected applicationConnected) {
            this.proxy = applicationConnected.isProxied() ? applicationConnected.getProxy() : currentNode;
        }
    }

    private class ComponentDetails {

        private final Map<String, ClientDetails> clients = new ConcurrentHashMap<>();


        public void applicationConnected(TopologyEvents.ApplicationConnected applicationConnected) {
            clients.put(applicationConnected.getClient(), new ClientDetails(applicationConnected));
        }

        public void applicationDisconnected(TopologyEvents.ApplicationDisconnected applicationDisconnected) {
            String proxy = applicationDisconnected.isProxied() ? applicationDisconnected.getProxy() : currentNode;
            ClientDetails old = clients.remove(applicationDisconnected.getClient());
            if (old != null && !proxy.equals(old.proxy)) {
                clients.put(applicationDisconnected.getClient(), old);
            }
        }

        public void updateProcessorStatus(ClientEventProcessorInfo eventProcessorStatus) {
            ClientDetails clientDetails = clients.get(eventProcessorStatus.getClientName());
            if (clientDetails != null) {
                clientDetails.eventProcessorInfo.put(eventProcessorStatus.getEventProcessorInfo().getProcessorName(),
                                                     eventProcessorStatus.getEventProcessorInfo());
            }
        }

        public void subscribeCommand(String client, String command) {
            ClientDetails clientDetails = clients.get(client);
            if (clientDetails != null) {
                clientDetails.commands.add(command);
            }
        }

        public void unsubscribeCommand(String client, String command) {
            ClientDetails clientDetails = clients.get(client);
            if (clientDetails != null) {
                clientDetails.commands.remove(command);
            }
        }


        public void subscribeQuery(String client, String query, String resultName) {
            ClientDetails clientDetails = clients.get(client);
            if (clientDetails != null) {
                clientDetails.queries.computeIfAbsent(query, q -> new CopyOnWriteArraySet<>()).add(resultName);
            }
        }

        public void unsubscribeQuery(String client, String query, String resultName) {
            ClientDetails clientDetails = clients.get(client);
            if (clientDetails != null) {
                Set<String> resultNames = clientDetails.queries.getOrDefault(query, Collections.emptySet());
                resultNames.remove(resultName);
                if (resultNames.isEmpty()) {
                    clientDetails.queries.remove(query);
                }
            }
        }
    }
}
