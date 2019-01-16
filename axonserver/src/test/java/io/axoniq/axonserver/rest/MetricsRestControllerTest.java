package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.CommandMetricsRegistry;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryMetricsRegistry;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.message.query.RoundRobinQueryHandlerSelector;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class MetricsRestControllerTest {
    private MetricsRestController testSubject;
    private CommandMetricsRegistry commandMetricsRegistry;
    private QueryMetricsRegistry queryMetricsRegistry;
    private ClientIdentification testclient;
    private ClientIdentification queryClient;

    @Before
    public void setUp()  {
        CommandRegistrationCache commandRegistrationCache = new CommandRegistrationCache();
        testclient = new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                                   "testclient");
        commandRegistrationCache.add("Sample", new CommandHandler<Object>(null,
                                                                          testclient, "testcomponent") {
            @Override
            public void dispatch(SerializedCommand request) {

            }

            @Override
            public void confirm(String messageId) {

            }

            @Override
            public int compareTo(@NotNull CommandHandler o) {
                return 0;
            }
        });
        commandMetricsRegistry = new CommandMetricsRegistry(new SimpleMeterRegistry(), new DefaultMetricCollector());

        QueryRegistrationCache queryRegistrationCache = new QueryRegistrationCache(new RoundRobinQueryHandlerSelector());
        queryClient = new ClientIdentification("context", "testclient");
        queryRegistrationCache.add(new QueryDefinition("context", "query"), "result",
                                   new QueryHandler<Object>(null,
                                                            queryClient, "testcomponent") {
            @Override
            public void dispatch(SubscriptionQueryRequest query) {

            }
        });
        queryMetricsRegistry = new QueryMetricsRegistry(new SimpleMeterRegistry(), new DefaultMetricCollector());
        testSubject = new MetricsRestController(commandRegistrationCache, commandMetricsRegistry,
                                                queryRegistrationCache, queryMetricsRegistry);
    }

    @Test
    public void getCommandMetrics() {
        List<CommandMetricsRegistry.CommandMetric> commands = testSubject.getCommandMetrics();
        assertEquals(1, commands.size());
        assertEquals(testclient.toString(), commands.get(0).getClientId());
        assertEquals(0, commands.get(0).getCount());
        commandMetricsRegistry.add("Sample", testclient, 1);
        commands = testSubject.getCommandMetrics();
        assertEquals(1, commands.size());
        assertEquals(testclient.toString(), commands.get(0).getClientId());
        assertEquals(1, commands.get(0).getCount());
    }

    @Test
    public void getQueryMetrics() {
        List<QueryMetricsRegistry.QueryMetric> queries = testSubject.getQueryMetrics();
        assertEquals(1, queries.size());
        assertEquals(queryClient.toString(), queries.get(0).getClientId());
        assertEquals(0, queries.get(0).getCount());

        queryMetricsRegistry.add(new QueryDefinition("context", "query"), queryClient, 50);

        queries = testSubject.getQueryMetrics();
        assertEquals(1, queries.size());
        assertEquals(queryClient.toString(), queries.get(0).getClientId());
        assertEquals(1, queries.get(0).getCount());
    }
}